# stop gap to allow AWS requests which have a non-empty payload...

function aws_string(dt::DateTime)
    y,m,d = Dates.yearmonthday(dt)
    h,mi,s = Dates.hour(dt),Dates.minute(dt),Dates.second(dt)
    yy = y < 0 ? @sprintf("%05i",y) : lpad(y,4,"0")
    mm = lpad(m,2,"0")
    dd = lpad(d,2,"0")
    hh = lpad(h,2,"0")
    mii = lpad(mi,2,"0")
    ss = lpad(s,2,"0")
    return "$yy-$mm-$(dd)T$hh:$mii:$ss"
end

aws_string(v::Bool) = v ? "True" : "False"
aws_string(v::Any) = string(v)

function get_utc_timestamp(addsecs=0;basic=false)
    dt = Dates.unix2datetime(Dates.datetime2unix(now(Dates.UTC)) + addsecs)
    dstr = aws_string(dt)
    if basic
        dstr = replace(dstr, Set(":-"), "")
    end
    return string(dstr, "Z")
end

sign(key, msg) = Crypto.hmacsha256_digest(msg, key)

function get_signature_key(key, datestamp, region, service)
    kDate = sign("AWS4" * key, datestamp)
    kRegion = sign(kDate, region)
    kService = sign(kRegion, service)
    kSigning = sign(kService, "aws4_request")
    return kSigning
end

function signature_version_4(env, service, method, host, action, payload)
    # inputs to request
    amzdate = get_utc_timestamp(; basic=true)
    datestamp = amzdate[1:searchindex(amzdate, "T")-1]

    # Task 1: canonical request
    canonical_uri = "/"
    canonical_querystring = ""
    canonical_headers = "host:" * host * "\n" * "x-amz-date:" * amzdate * "\n" * "x-amz-target:DynamoDB_20120810.$action" * "\n"
    signed_headers = "host;x-amz-date;x-amz-target"
    payload_hash = bytes2hex(Crypto.sha256(payload))

    canonical_request = method * "\n" * canonical_uri * "\n" * "" * "\n" *
        canonical_headers * "\n" * signed_headers * "\n" * payload_hash

    # Task 2: string to sign
    algorithm = "AWS4-HMAC-SHA256"
    region = replace(replace("ec2.us-east-1.amazonaws.com", r".amazonaws.com$", ""), r"^ec2.", "")
    credential_scope = datestamp * "/" * region * "/" * "dynamodb" * "/" * "aws4_request"
    string_to_sign = algorithm * "\n" * amzdate * "\n" * credential_scope * "\n" * bytes2hex(Crypto.sha256(canonical_request))

    # Task 3: calculate the signature
    signing_key = get_signature_key(env.aws_seckey, datestamp, region, service)
    signature = bytes2hex(sign(signing_key, string_to_sign))

    # Task 4: add signing information to request ##### how to return header to caller?
    authorization_header = algorithm * " " * "Credential=" * env.aws_id * "/" * credential_scope * ", " *
        "SignedHeaders=" * signed_headers * ", " * "Signature=" * signature

    headers = Dict("Authorization" => authorization_header,
               "X-Amz-Date" => amzdate,
               "Host" => host,
               "Content-Type" => "application/x-amz-json-1.0",
               "X-Amz-Target" => "DynamoDB_20120810.$action")

    return headers
end


function dynamo_execute(env, action, json_data; current_retry=0)
    host_base = replace(env.ep_host, r"^ec2.", "")
    host = "dynamodb.$(host_base)"

    body = JSON.json(json_data)
    amz_headers = signature_version_4(env, "dynamodb", "POST", host, action, body)

    function retry(msg :: AbstractString)
        if current_retry > 9
            error("Request failed after 10 retries: $msg")
        end

        sleep(2^current_retry * 0.05)
        return dynamo_execute(env, action, json_data; current_retry=current_retry+1)
    end

    resp = nothing
    try
        resp = Requests.post("https://$host/"; data=body, headers=amz_headers, timeout=env.timeout)
    catch e
        if isa(e, AbstractString)
            if ismatch(r"((Couldn't connect to server)|(Couldn't resolve host name))", e)
                @show :retry
                return retry(e)
            end
        end
        throw(e)
    end

    status = resp.status
    value = JSON.parse(bytestring(resp.data))

    if status == 400
        if haskey(value, "__type") && ismatch(r"ProvisionedThroughputExceededException$", value["__type"])
            return retry("ProvisionedThroughputExceededException")
        end
    end

    return (status, value)
end

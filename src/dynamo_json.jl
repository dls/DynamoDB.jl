#                _       _ _
#  ___  ___ _ __(_) __ _| (_)_______
# / __|/ _ \ '__| |/ _` | | |_  / _ \
# \__ \  __/ |  | | (_| | | |/ /  __/
# |___/\___|_|  |_|\__,_|_|_/___\___|

const null_attr = {"NULL" : true}
# TODO -- binary?
attribute_value(x :: Bool) = Dict{String, Any}("BOOL" => x)
attribute_value(x :: Real) = Dict{String, Any}("N" => x)
attribute_value(x :: String) = Dict{String, Any}("S" => x)

attribute_value(x :: Array) =
    Dict{String, Any}("L" => [attribute_value(e) for e=x])
attribute_value(x :: Set{Real}) =
    Dict{String, Any}("NS" => [attribute_value(e) for e=x])
attribute_value(x :: Set{String}) =
    Dict{String, Any}("SS" => [attribute_value(e) for e=x])
# TODO -- n-dimensional arrays

function attribute_value(x :: Dict)
    dict = Dict{String, Any}()
    for (k,v)=x
        dict[k] = null_or_val(v)
    endn
    Dict{String, Any}("M" => dict)
end

null_or_val(x) = x == nothing ? null_attr : attribute_value(x)

function attribute_value(x :: Dict)
    r = {}
    for (k,v)=x ; r[k] = null_or_val(v) ; end
    {"M" => r}
end

# general objects
function attribute_value(x)
    r = {}
    for e=fieldnames(x) ; r[string(e)] = null_or_val(x.(e)) ; end
    {"M" => r}
end


#      _                     _       _ _
#   __| | ___  ___  ___ _ __(_) __ _| (_)_______
#  / _` |/ _ \/ __|/ _ \ '__| |/ _` | | |_  / _ \
# | (_| |  __/\__ \  __/ |  | | (_| | | |/ /  __/
#  \__,_|\___||___/\___|_|  |_|\__,_|_|_/___\___|

function bool_value_to_attribute(val)
    if lowercase(val) == "false"
        return false
    elseif lowercase(val) == "true"
        return true
    else
        error("Received non-boolean value for boolean typed data: $val")
    end
end

function value_to_attribute(hash)
    ks = keys(hash)
    if length(ks) != 1
    end

    ty = ks[1]
    val = hash[ty]]

    if ty == "NULL"     ; return nothing
    elseif ty == "BOOL" ; return bool_value_to_attribute(lowercase(val))
    elseif ty == "N"    ; return parse(Float64, val)
    elseif ty == "S"    ; return val
    elseif ty == "L"    ; return [value_to_attribute(e) for e=val]
    elseif ty == "NS"   ; return Set([parse(Float64, e) for e=val])
    elseif ty == "SS"   ; return Set(val)
    else                ; error("Unknown datatype value: $ty")
    end
end
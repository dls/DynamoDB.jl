# isdefined(Base, :__precompile__) && __precompile__()

module DynamoDB

using AWS
using HTTPClient.HTTPC
import JSON

include("crypto.jl")

# TODO -- kill this file when AWS.jl adds support for specified payloads
include("dynamo_requests.jl")

include("dynamo_json.jl")
include("dynamo_dsl.jl")
include("dynamo.jl")

# package code goes here

end # module

using DynamoDB
using Base.Test

tic()
include("dynamo_json.jl")
include("dynamo_dsl.jl")
toc()

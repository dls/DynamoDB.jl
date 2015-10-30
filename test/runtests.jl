#using DynamoDB
using Base.Test

type Foo
    a
    b
end

tic()
include("dynamo_json.jl")
include("dynamo_dsl.jl")
include("dynamo.jl")
include("dynamo_row_ops.jl")
toc()

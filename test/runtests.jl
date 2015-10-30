module DynamoDBUnitTests
using Base.Test

type Foo
    a
    b
end

include("dynamo_json.jl")
include("dynamo_dsl.jl")
include("dynamo.jl")
include("dynamo_row_ops.jl")
end


using DynamoDB
using Base.Test
include("integration_tests.jl")
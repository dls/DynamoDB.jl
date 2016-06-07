# isdefined(Base, :__precompile__) && __precompile__()

module DynamoDB

using AWS
using Requests
import JSON

include("crypto.jl")

# TODO -- kill this file when AWS.jl adds support for specified payloads
include("dynamo_requests.jl")

include("dynamo_json.jl")
include("dynamo_dsl.jl")

include("dynamo.jl")
include("dynamo_row_ops.jl")

# package code goes here

# core types for modeling dynamo tables/indexes
export DynamoTable, DynamoLocalIndex, DynamoGlobalIndex,
       dynamo_table, dynamo_local_index, dynamo_global_index,
       random_key

# core operations
export get_item, put_item, update_item, delete_item

# high-read operations (easy to go over your read budget using them)
export query, scan

# batched opperations (these have gotchas... maybe read the docs before using)
export batch_get_item, batch_write_item, batch_put_item, batch_delete_item


export attribute, attr, getindex
export no_conditions, size, begins_with, contains, is_string, is_string_set, is_real, is_real_set,
       is_binary, is_binary_set, is_bool, is_null, is_list, is_map, is_document, exists, not_exists,
       <, >, <=, >=, eq, !=, and, or, !, not, between
export get_or_else, append_to_list, assign, add_to_set, remove_from_set, delete

end # module

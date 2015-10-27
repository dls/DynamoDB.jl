#  ____                                      _____
# |  _ \ _   _ _ __   __ _ _ __ ___   ___   |_   _|   _ _ __   ___  ___
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \    | || | | | '_ \ / _ \/ __|
# | |_| | |_| | | | | (_| | | | | | | (_) |   | || |_| | |_) |  __/\__ \
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/    |_| \__, | .__/ \___||___/
#        |___/                                    |___/|_|


# TODO -- exponential backoff algorithm with (logged?) warnings
# TODO -- optional whitelist and blacklist attribute values for persisting objects
# TODO -- version columns + transactions -- perhaps in a higher level library?


immutable DynamoTable
    ty :: Type
    name :: AbstractString
    hash_key_name :: AbstractString
    range_key_name :: Union{AbstractString, Void}

    aws_env # security credentials, etc
end

dynamo_table(ty :: Type, name, hash_key_name, range_key_name; env=nothing) =
    DynamoTable(ty, string(name), string(hash_key_name), range_key_name == nothing ? nothing : string(range_key_name), env)
dynamo_table(ty :: Type, name, hash_key_name; env=nothing) =
    DynamoTable(ty, string(name), string(hash_key_name), nothing, env)


immutable DynamoLocalIndex
    parent :: DynamoTable
    index_name :: AbstractString
    range_key_name :: AbstractString
end

dynamo_local_index(parent :: DynamoTable, index_name, range_key_name) =
    DynamoLocalIndex(parent, string(index_name), string(range_key_name))


immutable DynamoGlobalIndex
    parent :: DynamoTable
    index_name :: AbstractString
    hash_key_name :: AbstractString
    range_key_name :: Union{AbstractString, Void}
end

dynamo_global_index(parent :: DynamoTable, index_name, hash_key_name, range_key_name) =
    DynamoGlobalIndex(parent, string(index_name), string(hash_key_name), range_key_name == nothing ? nothing : string(range_key_name))
dynamo_global_index(parent :: DynamoTable, index_name, hash_key_name) =
    DynamoGlobalIndex(parent, string(index_name), string(hash_key_name), nothing)


function _keydict(hashname, hashval, rangename, rangeval)
    if rangename == nothing
        if rangeval != nothing
            error("tried to pass a non-null range value for a rangeless table $hashval, $rangeval")
        end
        return Dict{AbstractString, Any}(hashname => null_or_val(hashval))
    else
        return Dict{AbstractString, Any}(hashname => null_or_val(hashval), rangename => null_or_val(rangeval))
    end
end

keydict(table :: DynamoTable, key, range=nothing) =
    _keydict(table.hash_key_name, key, table.range_key_name, range)

function keydict(idx :: DynamoLocalIndex, key, range=nothing)
    if range == nothing
        error("Attempt to use a local secondary index to access a table via hash key... use the table instead")
    else
        return Dict{AbstractString, Any}(idx.table.hash_key_name => null_or_val(key),
                                         idx.range_key_name => null_or_val(range))
    end
end

keydict(idx :: DynamoGlobalIndex, key, range=nothing) =
    _keydict(idx.hash_key_name, key, idx.range_key_name, range)



# TODO: ... looks like there's not a standardized way to log in julia?
# will read up on the logging options and get back to this

immutable ConsumedCapacity
    value :: AbstractString
end
CC_INDEXES = ConsumedCapacity("INDEXES")
CC_TOTAL = ConsumedCapacity("TOTAL")
CC_NONE = ConsumedCapacity("NONE")


function set_expression_names_and_values(request_map, refs)
    if length(refs.attrs) != 0
       request_map["ExpressionAttributeNames"] = refs.attrs
    end
    if length(refs.vals) != 0
        request_map["ExpressionAttributeValues"] = refs.vals
    end
end


#     _    ____ ___      ____      _   ___ _
#    / \  |  _ \_ _|_   / ___| ___| |_|_ _| |_ ___ _ __ ___
#   / _ \ | |_) | |(_) | |  _ / _ \ __|| || __/ _ \ '_ ` _ \
#  / ___ \|  __/| | _  | |_| |  __/ |_ | || ||  __/ | | | | |
# /_/   \_\_|  |___(_)  \____|\___|\__|___|\__\___|_| |_| |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html

function get_item_query_dict(table :: DynamoTable, key, range, consistent_read, only_returning)
    request_map = Dict{Any, Any}("TableName" => table.name,
                                 "ConsistentRead" => consistent_read,
                                 "Key" => keydict(table, key, range))
    # TODO: "ReturnConsumedCapacity"

    if only_returning != nothing
        refs = refs_tracker()
        request_map["ProjectionExpression"] = join([write_expression(refs, e) for e=only_returning], ", ")
        request_map["ExpressionAttributeNames"] = refs.attrs
    end

    request_map
end

function get_item(table :: DynamoTable, key, range=nothing;
                  consistant_read=true, only_returning=nothing :: Union{Void, Array{DynamoReference}})
    request_map = get_item_query_dict(table, key, range, consistant_read, only_returning)

    (status, res) = dynamo_execute(table.aws_env, "GetItem", request_map)
    if haskey(res, "Item")
        return value_from_attributes(table.ty, res["Item"])
    end
end




#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  ____        _       _      ____      _   ___ _
# | __ )  __ _| |_ ___| |__  / ___| ___| |_|_ _| |_ ___ _ __ ___
# |  _ \ / _` | __/ __| '_ \| |  _ / _ \ __|| || __/ _ \ '_ ` _ \
# | |_) | (_| | || (__| | | | |_| |  __/ |_ | || ||  __/ | | | | |
# |____/ \__,_|\__\___|_| |_|\____|\___|\__|___|\__\___|_| |_| |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html

# note: you can only fetch 100 items at a time with this API
# TODO: multiplex calls to fix this client side?

type BatchGetItemPart
    table :: DynamoTable
    keys
    only_returning
    consistant_read
end

batch_get_item_part(table :: DynamoTable, keys...;
                    only_returning=nothing :: Union{Void, Array{DynamoReference}}, consistant_read=true) =
    BatchGetItemPart(table, keys, only_returning, consistant_read)

function batch_get_item_dict(arr :: Array{BatchGetItemPart})
    m = Dict()

    for part=arr
        request_map = Dict{Any, Any}("ConsistentRead" => part.consistant_read,
                                     "Keys" => [keydict(part.table, e...) for e=part.keys])
        # TODO: "ReturnConsumedCapacity"

        if part.only_returning != nothing
            refs = refs_tracker()
            request_map["ProjectionExpression"] = join([write_expression(refs, e) for e=part.only_returning], ", ")
            request_map["ExpressionAttributeNames"] = refs.attrs
        end

        m[part.table.name] = request_map
    end

    Dict("RequestItems" => m)
end

function batch_get_item(arr :: Array{BatchGetItemPart})
    request_map = batch_get_item_dict(arr)

    (status, res) = dynamo_execute(arr[1].table.aws_env, "BatchGetItem", request_map)

    type_lookup = Dict()
    for e = arr
        type_lookup[e.table.name] = e.table.ty
    end

    result = []
    for (name, list)=res["Responses"]
        ty = type_lookup[name]
        for e=list
            push!(result, value_from_attributes(ty, e))
        end
    end

    result
end

# helper function for the single table case
batch_get_item(table :: DynamoTable, keys...;
               only_returning=nothing :: Union{Void, Array{DynamoReference}}, consistant_read=true) =
    batch_get_item([batch_get_item_part(table :: DynamoTable, keys...; only_returning=only_returning, consistant_read=consistant_read)])



#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  ____        _   ___ _
# |  _ \ _   _| |_|_ _| |_ ___ _ __ ___
# | |_) | | | | __|| || __/ _ \ '_ ` _ \
# |  __/| |_| | |_ | || ||  __/ | | | | |
# |_|    \__,_|\__|___|\__\___|_| |_| |_|

function put_item_dict(table :: DynamoTable, item;
                       conditional_expression=nothing, return_old=false)
    request_map = Dict("TableName" => table.name,
                       "Item" => attribute_value(item)["M"])

    if conditional_expression != nothing
        refs = refs_tracker()
        request_map["ConditionExpression"] = write_expression(refs, conditional_expression)
        set_expression_names_and_values(request_map, refs)
    end

    if return_old
        request_map["ReturnValues"] = "ALL_OLD"
    end

    # TODO: ReturnConsumedCapacity ?
    # TODO: ReturnItemCollectionMetrics ?

    request_map
end

function put_item(table :: DynamoTable, item; conditional_expression=nothing, return_old=false)
    request_map = put_item_dict(table, item; conditional_expression=conditional_expression, return_old=return_old)

    (status, res) = dynamo_execute(table.aws_env, "PutItem", request_map)

    if return_old && haskey(res, "Attributes")
        return value_from_attributes(table.ty, res["Attributes"])
    end
end





#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  ____        _       _  __        __    _ _       ___ _
# | __ )  __ _| |_ ___| |_\ \      / / __(_) |_ ___|_ _| |_ ___ _ __ ___
# |  _ \ / _` | __/ __| '_ \ \ /\ / / '__| | __/ _ \| || __/ _ \ '_ ` _ \
# | |_) | (_| | || (__| | | \ V  V /| |  | | ||  __/| || ||  __/ | | | | |
# |____/ \__,_|\__\___|_| |_|\_/\_/ |_|  |_|\__\___|___|\__\___|_| |_| |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html

# write and/or delete up to 25 items at a time
# NOTE: you can't update items using this API call... use update_item instead


# TODO: ReturnConsumedCapacity
# TODO: ReturnItemCollectionMetrics

type BatchWriteItemPart
    table :: DynamoTable
    keys_to_delete
    items_to_write
end
batch_write_item_part(table :: DynamoTable, keys_to_delete, items_to_write) =
    BatchWriteItemPart(table, keys_to_delete, items_to_write)
batch_delete_part(table :: DynamoTable, keys...) =
    BatchWriteItemPart(table, keys, [])
batch_put_part(table :: DynamoTable, items...) =
    BatchWriteItemPart(table, [], items)


function batch_write_item_dict(parts :: Array{BatchWriteItemPart})
    dicts = []

    current_ct = 0
    current_dict = Dict()

    function add_op(table_name, op)
        if current_ct == 25
            push!(dicts, Dict("RequestItems" => current_dict))

            current_ct = 0
            current_dict = Dict()
        end

        if !haskey(current_dict, table_name)
            current_dict[table_name] = []
        end
        ops = current_dict[table_name]

        push!(ops, op)
        current_ct += 1
    end

    for p=parts
        for e=p.keys_to_delete
            add_op(p.table.name, Dict("DeleteRequest" => Dict("Key" => keydict(p.table, e...))))
        end
        for e=p.items_to_write
            add_op(p.table.name, Dict("PutRequest" => Dict("Item" => attribute_value(e)["M"])))
        end
    end

    push!(dicts, Dict("RequestItems" => current_dict))
    dicts
end

function batch_write_item(parts :: Array{BatchWriteItemPart})
    dicts = batch_write_item_dict(parts)

    if length(dicts) != 1
        # TODO:
        error("batch_write_item is limited to 25 items by the DynamoDB official API... and this library doesn't (yet) support multiplexing")
    end

    # TODO: ReturnConsumedCapacity
    # TODO: ReturnItemCollectionMetrics

    (status, res) = dynamo_execute(parts[1].table.aws_env, "BatchWriteItem", dicts[1])
end

# helper/simpler methods
batch_put_item(table :: DynamoTable, items...) =
    batch_write_item([batch_put_part(table, items...)])
batch_delete_item(table :: DynamoTable, keys...) =
    batch_write_item([batch_delete_part(table, keys...)])






#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  _   _           _       _       ___ _
# | | | |_ __   __| | __ _| |_ ___|_ _| |_ ___ _ __ ___
# | | | | '_ \ / _` |/ _` | __/ _ \| || __/ _ \ '_ ` _ \
# | |_| | |_) | (_| | (_| | ||  __/| || ||  __/ | | | | |
#  \___/| .__/ \__,_|\__,_|\__\___|___|\__\___|_| |_| |_|
#       |_|


const RETURN_NONE = "NONE"
const RETURN_ALL_OLD = "ALL_OLD"
const RETURN_UPDATED_OLD = "UPDATED_OLD"
const RETURN_ALL_NEW = "ALL_NEW"
const RETURN_UPDATED_NEW = "UPDATED_NEW"

function update_item_dict(table :: DynamoTable, key, range, update_expression;
                          conditions=nothing, returning=RETURN_NONE)
    request_map = Dict("TableName" => table.name,
                       "Key" => keydict(table, key, range),
                       "ReturnValues" => returning)

    # ReturnConsumedCapacity ==> INDEXES | TOTAL | NONE

    refs = refs_tracker()
    request_map["UpdateExpression"] = serialize_updates(update_expression, refs)
    if conditions != nothing
        request_map["ConditionExpression"] = serialize_expression(conditions, refs)
    end
    set_expression_names_and_values(request_map, refs)

    request_map
end

function update_item(table :: DynamoTable, key, range, update_expression :: Array;
                     conditions=nothing, returning=RETURN_NONE)
    request_map = update_item_dict(table, key, range, update_expression; conditions=nothing)

    # TODO: run it
    resp = Dict()

    (status, res) = dynamo_execute(table.aws_env, "UpdateItem", request_map)

    # TODO: only on success...

    if returning == RETURN_ALL_OLD || returning == RETURN_ALL_NEW
        value_from_attributes(table.ty, res["Attributes"])
    elseif returning == RETURN_UPDATED_OLD || returning == RETURN_UPDATED_NEW
        value_from_attributes(Dict, res["Attributes"])
    end
end


update_item(table :: DynamoTable, key, update_expression :: Array;
            conditions=nothing, returning=RETURN_NONE) =
    update_item(table, key, nothing, update_expression; conditions=conditions, returning=returning)


update_item{T <: DynamoUpdateExpression}(table :: DynamoTable, key, range, update_expression :: T;
            conditions=nothing, returning=RETURN_NONE) =
    update_item(table, key, range, [update_expression]; conditions=conditions, returning=returning)

update_item{T <: DynamoUpdateExpression}(table :: DynamoTable, key, update_expression :: T;
            conditions=nothing, returning=RETURN_NONE) =
    update_item(table, key, nothing, [update_expression]; conditions=conditions, returning=returning)



#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  ____       _      _       ___ _
# |  _ \  ___| | ___| |_ ___|_ _| |_ ___ _ __ ___
# | | | |/ _ \ |/ _ \ __/ _ \| || __/ _ \ '_ ` _ \
# | |_| |  __/ |  __/ ||  __/| || ||  __/ | | | | |
# |____/ \___|_|\___|\__\___|___|\__\___|_| |_| |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html

function delete_item_dict(table :: DynamoTable, key, range=nothing; conditions=nothing, return_old=false)
    # "ConditionExpression"

    request_map = Dict("TableName" => table.name,
                       "Key" => keydict(table, key, range))

    if return_old
        request_map["ReturnValues"] = "ALL_OLD"
    end

    refs = refs_tracker()
    if conditions != nothing
        request_map["ConditionExpression"] = serialize_expression(conditions, refs)
        set_expression_names_and_values(request_map, refs)
    end
end


function delete_item(table :: DynamoTable, key, range=nothing; conditions=nothing, return_old=false)
    request_map = delete_item_dict(table, key, range; conditions=conditions, return_old=return_old)

    (status, res) = dynamo_execute(table.aws_env, "DeleteItem", request_map)

    if return_old
        return value_from_attributes(table.ty, res["Attributes"])
    end
end



#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#   ___
#  / _ \ _   _  ___ _ __ _   _
# | | | | | | |/ _ \ '__| | | |
# | |_| | |_| |  __/ |  | |_| |
#  \__\_\\__,_|\___|_|   \__, |
#                        |___/

function query(table :: DynamoTable)
    Dict{AbstractString, Any}("TableName" => table.name,
                      "ConsistentRead" => true,
                      "ExclusiveStartKey" => 2,
                      "ExpressionAttributeNames" => 3,
                      "ExpressionAttributeValues" => 4,
                      "FilterExpression" => 5,
                      "KeyConditionExpression" => 6,
                      "Limit" => 7,
                      "ProjectionExpression" => 8,
                      "ScanIndexForward" => 9)

# Select ==> ALL_ATTRIBUTES | ALL_PROJECTED_ATTRIBUTES | SPECIFIC_ATTRIBUTES | COUNT
end

# set "IndexName"



#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  ____
# / ___|  ___ __ _ _ __
# \___ \ / __/ _` | '_ \
#  ___) | (_| (_| | | | |
# |____/ \___\__,_|_| |_|

function scan(table :: DynamoTable)
    Dict{AbstractString, Any}("TableName" => table.name,
                      "ConsistentRead" => 3,
                      "ExclusiveStartKey" => 4,
                      "ExpressionAttributeNames" => 4,
                      "ExpressionAttributeValues" => 5,
                      "FilterExpression" => 6,
                      "IndexName" => 888,
                      "Limit" => 9,
                      "ProjectionExpression" => 10,
                      "ReturnConsumedCapacity" => 11,
                      "Segment" => 12,
                      "TotalSegments" => 13,
                      "Select" => 13)
end

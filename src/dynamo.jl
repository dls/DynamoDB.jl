#  ____                                      _____
# |  _ \ _   _ _ __   __ _ _ __ ___   ___   |_   _|   _ _ __   ___  ___
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \    | || | | | '_ \ / _ \/ __|
# | |_| | |_| | | | | (_| | | | | | | (_) |   | || |_| | |_) |  __/\__ \
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/    |_| \__, | .__/ \___||___/
#        |___/                                    |___/|_|


# TODO -- optional whitelist and blacklist attribute values for persisting objects
# TODO -- version columns + transactions -- perhaps in a higher level library?

immutable DynamoTable
    ty :: Type
    name :: AbstractString
    hash_key_name :: AbstractString
    range_key_name :: Union{AbstractString, Void}
end

dynamo_table(ty :: Type, name, hash_key_name, range_key_name) =
    DynamoTable(ty, string(name), string(hash_key_name), range_key_name == nothing ? nothing : string(range_key_name))
dynamo_table(ty :: Type, name, hash_key_name) =
    DynamoTable(ty, string(name), string(hash_key_name), nothing)

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
    dict = get_item_query_dict(table, key, range, consistant_read, only_returning)

    res = Dict() # TODO: run it

    value_from_attribute(table.ty, res["Item"])
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
    dict = batch_get_item_dict(arr)

    res = Dict() # TODO: run it

    type_lookup = Dict()
    for e = arr
        type_lookup[e.table.name] = e.table.ty
    end

    result = []
    for (name, list)=res["Responses"]
        ty = type_lookup[name]
        for e=list
            push!(result, value_from_attribute(ty, e))
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
    request_map = Dict("Table" => table.name,
                       "Item" => attribute_value(item)["M"])

    if conditional_expression != nothing
        refs = refs_tracker()
        request_map["ConditionExpression"] = write_expression(refs, conditional_expression)
        request_map["ExpressionAttributeNames"] = refs.attrs
        request_map["ExpressionAttributeValues"] = refs.vals
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

    # TODO: run
    res = Dict()

    if return_old
        return value_from_attribute(table.ty, res["Attributes"])
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
# NOTE: you can't update items using this API call... use put_item instead :(


# TODO: ReturnConsumedCapacity
# TODO: ReturnItemCollectionMetrics

function batch_write_item()
end


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


function update_item(table :: DynamoTable, key, range)
#    Dict{AbstractString, Any}("TableName" => table.name,
#                      "Key" => keydict(table, key, range),
#                      "ReturnConsumedCapacity" => "NONE",
#                      "ConditionExpression" => 3,
#                      "UpdateExpression" => 6,
#                      "ExpressionAttributeNames" => 4,
#                      "ExpressionAttributeValues" => 5,
#                      "ReturnValues" => "NONE")

# ReturnConsumedCapacity ==> INDEXES | TOTAL | NONE
# ReturnValues ==> NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
end


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

function delete_item(table :: DynamoTable, key, range=nothing)
    # "ConditionExpression"
    # "ReturnValues"

    Dict{AbstractString, Any}("TableName" => table.name,
                      "Key" => keydict(table, key, range),
                      "ConditionExpression" => 3,
                      "ExpressionAttributeNames" => 4,
                      "ExpressionAttributeValues" => 5)
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

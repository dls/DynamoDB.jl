include("dynamo_json.jl")
include("dynamo_dsl.jl")

#  ____                                      _____
# |  _ \ _   _ _ __   __ _ _ __ ___   ___   |_   _|   _ _ __   ___  ___
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \    | || | | | '_ \ / _ \/ __|
# | |_| | |_| | | | | (_| | | | | | | (_) |   | || |_| | |_) |  __/\__ \
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/    |_| \__, | .__/ \___||___/
#        |___/                                    |___/|_|

# TODO -- optional whitelist and blacklist

immutable DynamoTable
    ty :: Type
    name :: String
    hash_key_name :: String
    range_key_name :: String
end

immutable DynamoLocalIndex
    parent :: DynamoTable
    index_name :: String
    range_key_name :: String
end

immutable DynamoGlobalIndex
    parent :: DynamoTable
    index_name :: String
    hash_key_name :: String
    range_key_name :: String
end


function _keydict(hashname, hashval, rangename, rangeval)
    if rangename == nothing
        if rangeval != nothing
            error("tried to pass a non-null range value for a rangeless table")
        end
        return Dict{String, Any}(hashname => hashval)
    else
        return Dict{String, Any}(hashname => hashval, rangename => rangeval)
    end
end

keydict(table :: DynamoTable, key, range=nothing) =
    _keydict(table.hash_key_name, key, table.range_key_name, range)

function keydict(idx :: DynamoLocalIndex, key, range=nothing)
    if range == nothing
        error("Attempt to use a local secondary index to access a table via hash key... use the table instead")
    else
        return Dict{String, Any}(idx.table.hash_key_name => key,
                                 idx.range_key_name => range)
    end
end

keydict(idx :: DynamoGlobalIndex, key, range=nothing) =
    _keydict(idx.hash_key_name, key, idx.range_key_name, range)




#     _    ____ ___      ____      _   ___ _
#    / \  |  _ \_ _|_   / ___| ___| |_|_ _| |_ ___ _ __ ___
#   / _ \ | |_) | |(_) | |  _ / _ \ __|| || __/ _ \ '_ ` _ \
#  / ___ \|  __/| | _  | |_| |  __/ |_ | || ||  __/ | | | | |
# /_/   \_\_|  |___(_)  \____|\___|\__|___|\__\___|_| |_| |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html


function get_item_query_dict(table :: DynamoTable, key, range)
    # "ReturnConsumedCapacity" => ?
    # "ConsistentRead" => ?

    # "ExpressionAttributeNames" => ?
    # "ProjectionExpression" => ?
    Dict{Any, Any}("TableName" => table.name,
                   "Key" => keydict(table, key, range))
end

function get_item(table :: DynamoTable, key, range=nothing; consistant_read=true)
    dict = query_dict(get)
    dict["ConsistentRead"] = consistant_read

    res = Dict() # TODO: run it

    item = res["Item"]
    init_vals = [value_to_attribute(item[string(e)]) for e=fieldnames(get.table.ty)]
    table.ty(init_vals...)
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

# TODO


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

function put_item(table :: DynamoTable, val)
    Dict{String, Any}("Table" => table.name,
                      "Item" => attribute_value(val))
end

function put_item(table :: DynamoTable, val, expr :: ConditionalExpression)
    s = serialize_expression(expr)
    Dict{String, Any}("Table" => table.name,
                      "Item" => attribute_value(val),
                      "ConditionExpression" => s.expression,
                      "ExpressionAttributeNames" => expr.expression_attrs,
                      "ExpressionAttributeValues" => expr.expression_vals)
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
    Dict{String, Any}("TableName" => table.name,
                      "Key" => keydict(table, key, range),
                      "ReturnConsumedCapacity" => "NONE",
                      "ConditionExpression" => 3,
                      "UpdateExpression" => 6,
                      "ExpressionAttributeNames" => 4,
                      "ExpressionAttributeValues" => 5,
                      "ReturnValues" => "NONE",

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

    Dict{String, Any}("TableName" => table.name,
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
    Dict{String, Any}("TableName" => table.name,
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
    Dict{String, Any}("TableName" => table.name,
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




#     _    ____ ___
#    / \  |  _ \_ _|_
#   / _ \ | |_) | |(_)
#  / ___ \|  __/| | _
# /_/   \_\_|  |___(_)
#  _____     _     _
# |_   _|_ _| |__ | | ___  ___
#   | |/ _` | '_ \| |/ _ \/ __|
#   | | (_| | |_) | |  __/\__ \
#   |_|\__,_|_.__/|_|\___||___/

function create_table()
    Dict{String, Any}("TableName" => 0,
                      "AttributeDefinitions" => 1,
                      "KeySchema" => 2,
                      "ProvisionedThroughput" => 3,
                      "GlobalSecondaryIndexes" => 4,
                      "LocalSecondaryIndexes" => 5,
                      "StreamSpecification" => 6)
end

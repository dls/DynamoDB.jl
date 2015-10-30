function set_expression_names_and_values(request_map, refs)
    if length(refs.attrs) != 0
       request_map["ExpressionAttributeNames"] = refs.attrs
    end
    if length(refs.vals) != 0
        request_map["ExpressionAttributeValues"] = refs.vals
    end
end

load_row(table, attr_dict; is_old=false) =
    after_load(table.extension, table, value_from_attributes(table.ty, res["Item"]);
               is_old=is_old)

attributes_to_write(table, item) =
    transform_attrs_for_write(table.extension, table, attribute_value(item)["M"])

updates_to_send(table, updates) =
    transform_update_expression(table.extension, table, updates)


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
    check_status(status, res)

    if haskey(res, "Item")
        return load_row(table, res["Item"])
    else
        return nothing
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

    # TODO: "ReturnConsumedCapacity"

    for part=arr
        request_map = Dict{Any, Any}("ConsistentRead" => part.consistant_read,
                                     "Keys" => [keydict(part.table, e...) for e=part.keys])

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
    check_status(status, res)

    table_lookup = Dict()
    for e = arr
        type_lookup[e.table.name] = e.table
    end

    result = []
    for (name, list)=res["Responses"]
        table = table_lookup[name]
        for e=list
            item = load_row(table, e)
            if item != nothing
                push!(result, item)
            end
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
                       conditions=no_conditions() :: CEBoolean, return_old=false)
    request_map = Dict("TableName" => table.name,
                       "Item" => attributes_to_write(table, item))

    conditional_expression = and(conditions, row_write_conditions(table.extension, table, item))
    if can_write_expression(conditional_expression)
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

function put_item(table :: DynamoTable, item; conditional_expression=no_conditions() :: CEBoolean, return_old=false)
    request_map = put_item_dict(table, item; conditional_expression=conditional_expression, return_old=return_old)

    (status, res) = dynamo_execute(table.aws_env, "PutItem", request_map)
    check_status(status, res)

    if return_old && haskey(res, "Attributes")
        return load_row(table.ty, res["Attributes"]; is_old=true)
    else
        return nothing
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
            push!(dicts, current_dict)

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
            if !can_batch_delete(p.table.extension, p.table)
                error("Tried to batch delete from table which doesn't support it. (The table extension probably requires writing conditions)")
            end
            add_op(p.table.name, Dict("DeleteRequest" => Dict("Key" => keydict(p.table, e...))))
        end
        for e=p.items_to_write
            if !can_batch_write(p.table.extension, p.table)
                error("Tried to batch write to table which doesn't support it. (The table extension probably requires writing conditions)")
            end
            add_op(p.table.name, Dict("PutRequest" => Dict("Item" => attributes_to_write(p.table, e))))
        end
    end

    push!(dicts, current_dict)
    dicts
end

function batch_write_item(parts :: Array{BatchWriteItemPart})
    dicts = batch_write_item_dict(parts)

    # TODO: ReturnConsumedCapacity
    # TODO: ReturnItemCollectionMetrics

    function process_dict(d, current_retry=0)
        (status, res) = dynamo_execute(parts[1].table.aws_env, "BatchWriteItem", Dict("RequestItems" => d))
        check_status(status, res)

        # TODO -- log with capacity stuffs
        # exponential backoff per the dynamodb docs
        if length(res["UnprocessedItems"]) != 0
            if current_retry > 9
                error("Request failed after 10 retries")
            end
            sleep(2^current_retry * 0.05)

            process_dict(res["UnprocessedItems"], current_retry+1)
        end
    end

    for d=dicts
        process_dict(d)
    end
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


# TODO -- enum of something? Julia doesn't support that though...
const RETURN_NONE = "NONE"
const RETURN_ALL_OLD = "ALL_OLD"
const RETURN_UPDATED_OLD = "UPDATED_OLD"
const RETURN_ALL_NEW = "ALL_NEW"
const RETURN_UPDATED_NEW = "UPDATED_NEW"

function update_item_dict(table :: DynamoTable, key, range, update_expression;
                          conditions=no_conditions() :: CEBoolean, returning=RETURN_NONE)
    request_map = Dict("TableName" => table.name,
                       "Key" => keydict(table, key, range),
                       "ReturnValues" => returning)

    # ReturnConsumedCapacity == INDEXES | TOTAL | NONE

    refs = refs_tracker()
    request_map["UpdateExpression"] = serialize_updates(updates_to_send(table, update_expression), refs)


    if can_write_expression(conditions)
        request_map["ConditionExpression"] = serialize_expression(conditions, refs)
    end

    set_expression_names_and_values(request_map, refs)
    request_map
end

function update_item(table :: DynamoTable, key, range, update_expression :: Array;
                     conditions=no_conditions() :: CEBoolean, returning=RETURN_NONE)
    request_map = update_item_dict(table, key, range, update_expression;
                                   conditions=conditions, returning=returning)

    # TODO: run it
    resp = Dict()

    (status, res) = dynamo_execute(table.aws_env, "UpdateItem", request_map)
    check_status(status, res)

    if returning == RETURN_ALL_NEW
        return load_row(table, res["Attributes"])
    elseif returning == RETURN_ALL_OLD
        return load_row(table, res["Attributes"]; is_old=true)
    elseif returning == RETURN_UPDATED_OLD || returning == RETURN_UPDATED_NEW
        value_from_attributes(Dict, res["Attributes"])
    end
end


update_item(table :: DynamoTable, key, update_expression :: Array;
            conditions=no_conditions() :: CEBoolean, returning=RETURN_NONE) =
    update_item(table, key, nothing, update_expression; conditions=conditions, returning=returning)


update_item{T <: DynamoUpdateExpression}(table :: DynamoTable, key, range, update_expression :: T;
            conditions=no_conditions() :: CEBoolean, returning=RETURN_NONE) =
    update_item(table, key, range, [update_expression]; conditions=conditions, returning=returning)

update_item{T <: DynamoUpdateExpression}(table :: DynamoTable, key, update_expression :: T;
            conditions=no_conditions() :: CEBoolean, returning=RETURN_NONE) =
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

function delete_item_dict(table :: DynamoTable, key, range=nothing;
                          conditions=no_conditions() :: CEBoolean, return_old=false)
    request_map = Dict("TableName" => table.name,
                       "Key" => keydict(table, key, range))

    if return_old
        request_map["ReturnValues"] = "ALL_OLD"
    end

    refs = refs_tracker()

    if can_write_expression(conditions)
        request_map["ConditionExpression"] = serialize_expression(conditions, refs)
        set_expression_names_and_values(request_map, refs)
    end

    request_map
end


function delete_item(table :: DynamoTable, key, range=nothing;
                     conditions=no_conditions() :: CEBoolean, return_old=false)
    request_map = delete_item_dict(table, key, range; conditions=conditions, return_old=return_old)

    (status, res) = dynamo_execute(table.aws_env, "DeleteItem", request_map)
    check_status(status, res)

    if return_old
        return load_row(table, res["Attributes"]; is_old=true)
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

# TODO: ReturnConsumedCapacity

const SELECT_ALL_ATTRIBUTES = "ALL_ATTRIBUTES"
const SELECT_ALL_PROJECTED_ATTRIBUTES = "ALL_PROJECTED_ATTRIBUTES"
const SELECT_SPECIFIC_ATTRIBUTES = "SPECIFIC_ATTRIBUTES"
const SELECT_COUNT = "COUNT"

function query_dict(table :: DynamoTable, hash_val, range_condition;
               filter=nothing :: Union{Void, CEBoolean}, projection=DynamoReference[] :: Array{DynamoReference},
               consistant_read=true, scan_index_forward=true, limit=nothing, index_name=nothing,
               select_type=nothing, start_key=nothing)
    refs = refs_tracker()
    request_map = Dict{AbstractString, Any}("TableName" => table.name,
                       "KeyConditionExpression" => serialize_expression(and(attr(table.hash_key_name) == hash_val,
                                                                            range_condition), refs))

    if start_key != nothing
        request_map["ExclusiveStartKey"] = start_key
    end
    # only write if value isn't the default value
    if consistant_read == false
        request_map["ConsistentRead"] = consistant_read
    end
    if scan_index_forward == false
        request_map["ScanIndexForward"] = scan_index_forward
    end

    if index_name != nothing
        request_map["IndexName"] = index_name
    end
    if length(projection) != 0
        request_map["ProjectionExpression"] = join([write_expression(refs, e) for e=projection], ", ")
    elseif select_type != nothing
        request_map["Select"] = select_type
    end
    if filter != nothing
        request_map["FilterExpression"] = serialize_expression(filter, refs)
    end
    if limit != nothing
        request_map["Limit"] = limit
    end
    set_expression_names_and_values(request_map, refs)

    request_map
end

function query(table :: DynamoTable, hash_val, range_condition = no_conditions() :: CEBoolean;
               filter=nothing :: Union{Void, CEBoolean}, projection=DynamoReference[] :: Array{DynamoReference},
               consistant_read=true, scan_index_forward=true, limit=nothing, index_name=nothing,
               select_type=nothing)

    function run_query_part(start_key)
        request_map = query_dict(table, hash_val, range_condition; filter=filter, projection=projection,
                                 consistant_read=consistant_read, scan_index_forward=scan_index_forward,
                                 limit=limit, index_name=index_name, select_type=select_type,
                                 start_key=start_key)

        (status, res) = dynamo_execute(table.aws_env, "Query", request_map)
        check_status(status, res)

        # TODO: potentially interesting return values?
        # Count -- number of items returned
        # ScannedCount -- number of items accessed

        for e=res["Items"]
            produce(value_from_attributes(table.ty, e))
        end

        if haskey(res, "LastEvaluatedKey")
            res["Items"] = nothing
            run_query_part(res["LastEvaluatedKey"])
        end
    end

    @task run_query_part(nothing)
end

query(table :: DynamoLocalIndex, range_condition;
      filter=nothing :: Union{Void, CEBoolean}, projection=[] :: Array{DynamoReference},
      consistant_read=true, scan_index_forward=true, limit=nothing, select_type=nothing) =
   query(table.parent, hash_val, range_condition; filter=filter, projection=projection,
         consistant_read=consistant_read, scan_index_forward=scan_index_forward,
         limit=limit, index_name=table.index_name, select_type=select_type)

# NOTE: consistant_reads aren't possible on global secondary indexes (hence the missing param)
query(table :: DynamoGlobalIndex, range_condition;
      filter=nothing :: Union{Void, CEBoolean}, projection=[] :: Array{DynamoReference},
      scan_index_forward=true, limit=nothing, select_type=nothing) =
   query(table.parent, hash_val, range_condition; filter=filter, projection=projection,
         consistant_read=false, scan_index_forward=scan_index_forward,
         limit=limit, index_name=table.index_name, select_type=select_type)


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

function scan_dict(table :: DynamoTable, filter = no_conditions() :: CEBoolean;
                   projection=DynamoReference[] :: Array{DynamoReference}, consistant_read=false, scan_index_forward=true,
                   limit=nothing, select_type=nothing, count=false, segment=nothing, total_segments=nothing,
                   index_name=nothing, start_key=nothing)

    # TODO: ReturnConsumedCapacity

    refs = refs_tracker()
    request_map = Dict{AbstractString, Any}("TableName" => table.name)

    if start_key != nothing
        request_map["ExclusiveStartKey"] = start_key
    end
    # only write if value isn't the default value (false)
    if consistant_read == true
        request_map["ConsistentRead"] = consistant_read
    end
    if scan_index_forward == false
        request_map["ScanIndexForward"] = scan_index_forward
    end
    if segment != nothing || total_segments != nothing
        if segment == nothing || total_segments == nothing
            error("you must specify BOTH segment and total_segments or neither")
        end
        request_map["Segment"] = segment
        request_map["TotalSegments"] = total_segments
    end

    if can_write_expression(filter)
        request_map["FilterExpression"] = serialize_expression(filter, refs)
    end
    if index_name != nothing
        request_map["IndexName"] = index_name
    end
    if length(projection) != 0
        request_map["ProjectionExpression"] = join([write_expression(refs, e) for e=projection], ", ")
    elseif count
        request_map["Select"] = "Count"
    elseif select_type != nothing
        request_map["Select"] = select_type
    end
    if limit != nothing
        request_map["Limit"] = limit
    end
    set_expression_names_and_values(request_map, refs)

    request_map
end

function scan(table :: DynamoTable, filter = no_conditions() :: CEBoolean;
              projection=DynamoReference[] :: Array{DynamoReference}, consistant_read=false, scan_index_forward=true,
              limit=nothing, select_type=nothing, count=false, segment=nothing, total_segments=nothing,
              index_name=nothing)

    function run_scan_part(start_key)
        request_map = scan_dict(table, filter;
                        projection=projection, consistant_read=consistant_read,
                        scan_index_forward=scan_index_forward, limit=limit, select_type=select_type,
                        count=count, segment=segment, total_segments=total_segments,
                        index_name=index_name, start_key=start_key)

        (status, res) = dynamo_execute(table.aws_env, "Scan", request_map)
        check_status(status, res)

        # TODO: potentially interesting return values?
        # Count -- number of items returned
        # ScannedCount -- number of items accessed

        for e=res["Items"]
            produce(value_from_attributes(table.ty, e))
        end

        if haskey(res, "LastEvaluatedKey")
            run_scan_part(res["LastEvaluatedKey"])
        end
    end

    @task run_scan_part(nothing)
end


scan(table :: DynamoLocalIndex, filter = no_conditions() :: CEBoolean;
     projection=DynamoReference[] :: Array{DynamoReference}, scan_index_forward=true,
     limit=nothing, select_type=nothing, count=false, segment=nothing, total_segments=nothing) =
    scan(table.parent, filter=filter;
         projection=projection, consistant_read=false, scan_index_forward=scan_index_forward,
         limit=limit, select_type=select_type, count=count, segment=segment, total_segments=total_segments,
         index_name=table.index_name)

scan(table :: DynamoGlobalIndex, filter = no_conditions() :: CEBoolean;
     projection=DynamoReference[] :: Array{DynamoReference}, scan_index_forward=true,
     limit=nothing, select_type=nothing, count=false, segment=nothing, total_segments=nothing) =
    scan(table.parent, filter=filter;
         projection=projection, consistant_read=false, scan_index_forward=scan_index_forward,
         limit=limit, select_type=select_type, count=count, segment=segment, total_segments=total_segments,
         index_name=table.index_name)

#  ____                                      _____
# |  _ \ _   _ _ __   __ _ _ __ ___   ___   |_   _|   _ _ __   ___  ___
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \    | || | | | '_ \ / _ \/ __|
# | |_| | |_| | | | | (_| | | | | | | (_) |   | || |_| | |_) |  __/\__ \
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/    |_| \__, | .__/ \___||___/
#        |___/                                    |___/|_|


# TODO -- optional whitelist and blacklist attribute values for persisting objects
# TODO -- version columns + transactions -- perhaps in a higher level library?

# TODO -- some way of talking about / logging consumed capacity would be nice
# TODO -- batch_get_item auto-multiplexing (100 item limit)
# TODO -- dynamo streaming API
# TODO -- dynamo table-level APIs (creation, deletion, etc)

# TODO -- simple transactions -- perhaps in a higher level library?

immutable DynamoTable
    ty :: Type
    name :: AbstractString
    hash_key_name :: AbstractString
    range_key_name :: Union{AbstractString, Void}

    aws_env # security credentials, etc
    version_attr
    required_attrs
    hidden_attrs
end

dynamo_table(ty :: Type, name, hash_key_name, range_key_name;
             env=nothing, version_attr=nothing, required_attrs=Set(), hidden_attrs=Set()) =
    DynamoTable(ty, string(name), string(hash_key_name), range_key_name == nothing ? nothing : string(range_key_name),
                env, version_attr, required_attrs, hidden_attrs)
dynamo_table(ty :: Type, name, hash_key_name;
             env=nothing, version_attr=nothing, required_attrs=Set(), hidden_attrs=Set()) =
    DynamoTable(ty, string(name), string(hash_key_name), nothing,
                env, version_attr, required_attrs, hidden_attrs)

attribute_value(x :: Dict, table :: DynamoTable) =
    attribute_value(x; hidden_attrs=table.hidden_attrs, required_attrs=table.required_attrs)

function table_conditions(table :: DynamoTable)
    if table.version_attr == nothing
        return no_conditions()
    end

    attr(table.version_attr)
end


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



# some helper functions
function check_status(code, resp)
    if code != 200
        error(code, resp)
    end
end

function set_expression_names_and_values(request_map, refs)
    if length(refs.attrs) != 0
       request_map["ExpressionAttributeNames"] = refs.attrs
    end
    if length(refs.vals) != 0
        request_map["ExpressionAttributeValues"] = refs.vals
    end
end

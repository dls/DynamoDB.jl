#  ____                                      _____
# |  _ \ _   _ _ __   __ _ _ __ ___   ___   |_   _|   _ _ __   ___  ___
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \    | || | | | '_ \ / _ \/ __|
# | |_| | |_| | | | | (_| | | | | | | (_) |   | || |_| | |_) |  __/\__ \
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/    |_| \__, | .__/ \___||___/
#        |___/                                    |___/|_|


# TODO -- some way of talking about / logging consumed capacity would be nice
# TODO -- batch_get_item auto-multiplexing (100 item limit)
# TODO -- dynamo streaming API
# TODO -- dynamo table-level APIs (creation, deletion, etc)

# TODO -- simple transactions -- in a higher level library?


# TODO -- optional whitelist and blacklist attribute values for persisting objects
# TODO -- version columns + transactions -- perhaps in a higher level library?

random_key() = string(Base.Random.uuid4())

immutable DynamoTable
    ty :: Type
    name :: AbstractString
    hash_key_name :: AbstractString
    range_key_name :: Union{AbstractString, Void}

    aws_env # security credentials, etc
    extension
end

dynamo_table(ty :: Type, name, hash_key_name, range_key_name; env=nothing, extension=nothing) =
    DynamoTable(ty, string(name), string(hash_key_name),
                range_key_name == nothing ? nothing : string(range_key_name), env, extension)
dynamo_table(ty :: Type, name, hash_key_name; env=nothing, extension=nothing) =
    DynamoTable(ty, string(name), string(hash_key_name), nothing, env, extension)


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





# subclass this type, and override these functions to define global
# custom behaviors for one or more of your dynamo tables
abstract DynamoExtension

row_write_conditions(extension, table :: DynamoTable, value) = no_conditions()
row_delete_conditions(extension, table :: DynamoTable) = no_conditions()

# filter columns, error on missing columns, etc
transform_attrs_for_write(extension, table, attrs) = attrs
transform_update_expression(extension, table, array) = array

# NOTE: batch operations cannot contain condition expressions
can_batch_write(extension, table :: DynamoTable) = true
can_batch_delete(extension, table :: DynamoTable) = true

# set up values, filter the row, etc
after_load(extension, table :: DynamoTable, item; is_old=false) = item






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
        return Dict{AbstractString, Any}(idx.parent.hash_key_name => null_or_val(key),
                                         idx.range_key_name => null_or_val(range))
    end
end

keydict(idx :: DynamoGlobalIndex, key, range=nothing) =
    _keydict(idx.hash_key_name, key, idx.range_key_name, range)



# TODO: ... looks like there's not a standardized way to log in julia?
# will read up on the logging options and get back to this

# immutable ConsumedCapacity
#     value :: AbstractString
# end
# CC_INDEXES = ConsumedCapacity("INDEXES")
# CC_TOTAL = ConsumedCapacity("TOTAL")
# CC_NONE = ConsumedCapacity("NONE")



# some helper functions
function check_status(code, resp)
    if code != 200
        error(code, resp)
    end
end

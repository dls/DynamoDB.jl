#  ____                                    ____  ____  _
# |  _ \ _   _ _ __   __ _ _ __ ___   ___ |  _ \/ ___|| |
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \| | | \___ \| |
# | |_| | |_| | | | | (_| | | | | | | (_) | |_| |___) | |___
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/|____/|____/|_____|
#        |___/

# This file defines some types and serializers which allow for the
# easy writing and manipulation of dynamodb conditional expressions
# ("only write if X is true"), key condition expressions ("only read
# objects from the disk which match X"), and filter expressions
# ("ignore values read from disk which don't match X").

# The reason this is preferable than just writing queries by hand as
# strings is that dynamo requires a strange serialization format for
# types, and has surprising keyword requirements for unescaped
# fieldnames. Doing it this way makes those bits non-issues.




# Dictionary of references, used to keep the serialization orderly and
# compact.
type DynamoAttrAndValReferences
    gensym_n :: Int64

    attrs
    attrs_reversed # used to avoid double-sending duplicate values

    vals
    vals_reversed # used to avoid double-sending duplicate values
end

function incr_gensym(w :: DynamoAttrAndValReferences)
    n = w.gensym_n
    w.gensym_n += 1
    n
end

refs_tracker() = DynamoAttrAndValReferences(1, "",
                                            Dict{String, Any}(), Dict{Any, String}(),
                                            Dict{String, Any}(), Dict{Any, String}())



# a reference to a dynamodb record field
abstract DynamoValue
abstract DynamoReference <: DynamoValue

immutable DynamoAttribute <: DynamoReference
    name :: String
end
attribute(name :: String) = DynamoAttribute(name)
attribute(name :: Symbol) = attribute(string(name))
attr(name :: String) = attribute(name)
attr(name :: Symbol) = attribute(name)

function write_expression(w :: DynamoAttrAndValReferences, attr :: DynamoAttribute)
    if haskey(w.attrs_reversed, attr)
        return w.attrs_reversed[attr]
    end

    name = "#$(incr_gensym(w))"
    w.attrs[name] = attr.name
    w.attrs_reversed[attr] = name
    name
end


# a reference to dynamo sub-documents... eg "foo.bar" in {foo => {bar => 3}}
immutable NestedDynamoAttribute <: DynamoReference
    attrs :: Array{DynamoAttribute}
end
attribute(names...) = NestedDynamoAttribute([attr(e) for e=names])
attr(names...) = attribute(names...)

write_expression(w :: DynamoAttrAndValReferences, nested :: NestedDynamoAttribute) =
    join([add_attr(w, e) for e=nested.attrs], ".")


immutable DynamoListElement <: DynamoReference
    attr :: DynamoReference
    idx
end
import Base.getindex
getindex(attr :: DynamoReference, idx :: Int64) = DynamoListElement(attr, idx)

write_expression(w :: DynamoAttrAndValReferences, v :: DynamoListElement) =
    "$(write_expression(v.attr))[$(v.idx)]"


immutable DynamoLiteralValue <: DynamoValue
    val
end
value_or_literal(a :: DynamoValue) = a
value_or_literal(other) = DynamoLiteralValue(other)

function write_expression(w :: DynamoAttrAndValReferences, val :: DynamoLiteralValue)
    if haskey(w.vals_reversed, val)
        return w.vals_reversed[val]
    end

    name = ":$(incr_gensym(w))"
    w.vals[name] = null_or_val(val.val)
    w.vals_reversed[val] = name
    name
end

#   ____                _ _ _   _                   _
#  / ___|___  _ __   __| (_) |_(_) ___  _ __   __ _| |
# | |   / _ \| '_ \ / _` | | __| |/ _ \| '_ \ / _` | |
# | |__| (_) | | | | (_| | | |_| | (_) | | | | (_| | |
#  \____\___/|_| |_|\__,_|_|\__|_|\___/|_| |_|\__,_|_|
#  _____                              _
# | ____|_  ___ __  _ __ ___  ___ ___(_) ___  _ __  ___
# |  _| \ \/ / '_ \| '__/ _ \/ __/ __| |/ _ \| '_ \/ __|
# | |___ >  <| |_) | | |  __/\__ \__ \ | (_) | | | \__ \
# |_____/_/\_\ .__/|_|  \___||___/___/_|\___/|_| |_|___/
#            |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html

abstract CEBoolean
abstract CEFnVal
typealias CEVal Union{DynamoValue, CEFnVal}

immutable CESize <: CEFnVal
    attr :: DynamoAttribute
end
size(attr :: DynamoAttribute) = CESize(attr)

immutable CEBeginsWith <: CEBoolean
    attr :: DynamoReference
    val
end
begins_with(attr :: DynamoReference, val) = CEBeginsWith(attr, value)

immutable CEContains <: CEBoolean
    attr :: DynamoReference
    val
end
contains(attr :: DynamoReference, val) = CEContains(attr, val)

immutable CEAttributeType <: CEBoolean
    attr :: DynamoReference
    ty :: String
end
is_string(attr :: DynamoReference) = CEAttributeType(attr, "S")
is_string_set(attr :: DynamoReference) = CEAttributeType(attr, "SS")
is_real(attr :: DynamoReference) = CEAttributeType(attr, "N")
is_real_set(attr :: DynamoReference) = CEAttributeType(attr, "NS")
is_binary(attr :: DynamoReference) = CEAttributeType(attr, "B")
is_binary_set(attr :: DynamoReference) = CEAttributeType(attr, "BS")
is_bool(attr :: DynamoReference) = CEAttributeType(attr, "BOOL")
is_null(attr :: DynamoReference) = CEAttributeType(attr, "NULL")
is_list(attr :: DynamoReference) = CEAttributeType(attr, "L")
is_map(attr :: DynamoReference) = CEAttributeType(attr, "M")

immutable CEExists <: CEBoolean
    attr :: DynamoReference
end
exists(attr :: DynamoReference) = CEExists(attr)

immutable CENotExists <: CEBoolean
    attr :: DynamoReference
end
not_exists(attr :: DynamoReference) = CENotExists(attr)

immutable CEBinaryOp <: CEBoolean
    op :: String
    lhs
    rhs
end
<(lhs :: CEVal, rhs) = CEBinaryOp("<", lhs, value_or_literal(rhs))
<(lhs, rhs :: CEVal) = CEBinaryOp("<", value_or_literal(lhs), rhs)

>(lhs :: CEVal, rhs) = CEBinaryOp(">", value_or_literal(lhs), rhs)
>(lhs, rhs :: CEVal) = CEBinaryOp(">", lhs, value_or_literal(rhs))

<=(lhs :: CEVal, rhs) = CEBinaryOp("<=", lhs, value_or_literal(rhs))
<=(lhs, rhs :: CEVal) = CEBinaryOp("<=", value_or_literal(lhs), rhs)

>=(lhs :: CEVal, rhs) = CEBinaryOp(">=", lhs, value_or_literal(rhs))
>=(lhs, rhs :: CEVal) = CEBinaryOp(">=", value_or_literal(lhs), rhs)

==(lhs :: CEVal, rhs) = CEBinaryOp("=", lhs, value_or_literal(rhs))
==(lhs, rhs :: CEVal) = CEBinaryOp("=", value_or_literal(lhs), rhs)

!=(lhs :: CEVal, rhs) = CEBinaryOp("<>", lhs, value_or_literal(rhs))
!=(lhs, rhs :: CEVal) = CEBinaryOp("<>", value_or_literal(lhs), rhs)

&&(lhs :: CEBoolean, rhs :: CEBoolean) = CEBinaryOp("AND", lhs, rhs)
||(lhs :: CEBoolean, rhs :: CEBoolean) = CEBinaryOp("OR", lhs, rhs)

immutable CENot <: CEBoolean
    exp :: CEBoolean
end

!(exp :: CEBoolean) = CENot(exp)
not(exp :: CEBoolean) = CENot(exp)

immutable CEBetween <: CEBoolean
    attr :: DynamoAttribute
    min
    max
end
between(attr :: DynamoAttribute, min, max) =
    CEBetween(attr, value_or_literal(min), value_or_literal(max))



# Okay, with that out of the way, let's get serializin'
write_expression(w :: DynamoAttrAndValReferences, e :: CESize) =
    "size($(write_expression(e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBeginsWith) =
    "begins_with($(write_expression(w, e.attr)), $(write_expression(w, e.val)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEContains) =
    "contains($(write_expression(w, e.attr)), $(write_expression(w, e.val)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEAttributeType) =
    "attribute_type($(write_expression(w, e.attr)), \"$(e.ty))\""
write_expression(w :: DynamoAttrAndValReferences, e :: CENotExists) =
    "attribute_not_exists($(write_expression(w, e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEExists) =
    "attribute_exists($(write_expression(w, e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBinaryOp) =
    "($(write_expression(w, e.lhs))) $e.op ($(write_expression(w, e.rhs))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBetween) =
    "($(write_expression(e.attr)) BETWEEN $(write_expression(e.min)) AND $(write_expression(e.max)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CENot) =
    "(NOT ($(write_expression(w, e.exp))))"

serialize_expression(expr :: ConditionalExpression, refs = refs_tracker()) =
    write_expression(refs, expr)



#  _   _           _       _
# | | | |_ __   __| | __ _| |_ ___
# | | | | '_ \ / _` |/ _` | __/ _ \
# | |_| | |_) | (_| | (_| | ||  __/
#  \___/| .__/ \__,_|\__,_|\__\___|
#       |_|
#  _____                              _
# | ____|_  ___ __  _ __ ___  ___ ___(_) ___  _ __  ___
# |  _| \ \/ / '_ \| '__/ _ \/ __/ __| |/ _ \| '_ \/ __|
# | |___ >  <| |_) | | |  __/\__ \__ \ | (_) | | | \__ \
# |_____/_/\_\ .__/|_|  \___||___/___/_|\___/|_| |_|___/
#            |_|

# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html

# SET
# REMOVE
# ADD
# DELETE


abstract DynamoUpdateExpression
abstract DUFnVal
typealias DUVal Union{DynamoValue, DUFnVal}

immutable DefaultValue <: DUVal
    attr :: DynamoReference
    val
end
get_or_else(attr :: DynamoReference, val) = DefaultValue(attr, val)

immutable ListAppend <: DynamoUpdateExpression
    attr :: DynamoReference
    val
end
append(attr :: DynamoReference, val) = ListAppend(attr, value_or_literal(val))

immutable AssignExpression < DynamoUpdateExpression
    attr :: DynamoReference
    val
end
assign(attr :: DynamoReference, val) = AssignExpression(attr, value_or_literal(val))

immutable SetAddExpression < DynamoUpdateExpression
    attr :: DynamoReference
    val
end
set_add(attr :: DynamoAttribute, val) = SetAddExpression(attr, value_or_literal(val))

immutable SetRemoveExpression < DynamoUpdateExpression
    attr :: DynamoReference
    val
end
set_remove(attr :: DynamoReference) = SetRemoveExpression(attr, value_or_literal(val))

immutable DeleteExpression < DynamoUpdateExpression
    attr :: DynamoReference
end
remove(attr :: DynamoReference) = DeleteExpression(attr)


type UpdateWriter
    refs :: DynamoAttrAndValReferences

    sets :: Array{Any}
    removes :: Array{Any}
    adds :: Array{Any}
    deletes :: Array{Any}
end

write_expression(w :: UpdateWriter, v :: DefaultValue) =
    "if_not_exists($(write_expression(w.refs, v.attr)), $(write_expression(w.refs, v.val)))"

write_update(w :: UpdateWriter, v :: ListAppend) =
    push!(w.sets, "$(write_expression(w, v.attr)) = list_append($(write_expression(w, v.attr)), $(add_val(v.val)))")

write_update(w :: UpdateWriter, v :: AssignExpression) =
    push!(w.sets, "$(write_expression(v.attr)) = $(write_expression(v.val))")

write_update(w :: UpdateWriter, v :: SetAddExpression) =
    push!(w.adds, "$(write_expression(v.attr)) $(write_expression(v.val))")

write_update(w :: UpdateWriter, v :: SetRemoveExpression) =
    push!(w.deletes, "$(write_expression(v.attr)) $(write_expression(v.val))")

write_update(w :: UpdateWriter, v :: DeleteExpression) =
    push!(w.removes, "$(write_expression(v.attr))")

function serialize_updates(arr :: Array{DynamoUpdateExpression}, refs = refs_tracker())
    w = UpdateWriter(refs, [], [], [], [])
    for e=arr
        write_update(w, e)
    end
    w
end

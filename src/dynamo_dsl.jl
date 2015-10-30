#  ____                                    ____  ____  _
# |  _ \ _   _ _ __   __ _ _ __ ___   ___ |  _ \/ ___|| |
# | | | | | | | '_ \ / _` | '_ ` _ \ / _ \| | | \___ \| |
# | |_| | |_| | | | | (_| | | | | | | (_) | |_| |___) | |___
# |____/ \__, |_| |_|\__,_|_| |_| |_|\___/|____/|____/|_____|
#        |___/

# This file defines some types and serializers which allow for the
# easy writing and manipulation of dynamodb conditional write
# expressions ("only write if X is true"), key condition expressions
# ("only read objects from the disk which match X"), and filter
# expressions ("ignore values read from disk which don't match X").

# The reason this is preferable than just writing queries by hand as
# strings is that dynamo requires a strange serialization format for
# types, and has surprising keyword requirements for unescaped
# fieldnames. Doing it this way makes those bits non-issues.






# a reference to a dynamodb record field
abstract DynamoValue
abstract DynamoReference <: DynamoValue

immutable DynamoAttribute <: DynamoReference
    name :: AbstractString
end
attribute(name :: AbstractString) = DynamoAttribute(name)
attribute(name :: Symbol) = attribute(string(name))
attr(name :: AbstractString) = attribute(name)
attr(name :: Symbol) = attribute(name)


# a reference to dynamo sub-documents... eg "foo.bar" in {foo => {bar => 3}}
immutable NestedDynamoAttribute <: DynamoReference
    attrs :: Array{DynamoAttribute}
end
attribute(names...) = NestedDynamoAttribute([attr(e) for e=names])
attr(names...) = attribute(names...)


immutable DynamoListElement <: DynamoReference
    attr :: DynamoReference
    idx
end
import Base.getindex
getindex(attr :: DynamoReference, idx :: Int64) = DynamoListElement(attr, idx)


immutable DynamoLiteralValue <: DynamoValue
    val
end
value_or_literal(a :: DynamoValue) = a
value_or_literal(other) = DynamoLiteralValue(other)



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

immutable CETrue <: CEBoolean ; end
no_conditions() = CETrue()

immutable CESize <: CEFnVal
    attr :: DynamoReference
end
import Base.size
size(attr :: DynamoReference) = CESize(attr)

immutable CEBeginsWith <: CEBoolean
    attr :: DynamoReference
    val
end
begins_with(attr :: DynamoReference, val) = CEBeginsWith(attr, value_or_literal(val))

immutable CEContains <: CEBoolean
    attr :: DynamoReference
    val
end
import Base.contains
contains(attr :: DynamoReference, val) = CEContains(attr, value_or_literal(val))

immutable CEAttributeType <: CEBoolean
    attr :: DynamoReference
    ty :: AbstractString
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
is_document(attr :: DynamoReference) = CEAttributeType(attr, "M")

immutable CEExists <: CEBoolean
    attr :: DynamoReference
end
exists(attr :: DynamoReference) = CEExists(attr)

immutable CENotExists <: CEBoolean
    attr :: DynamoReference
end
not_exists(attr :: DynamoReference) = CENotExists(attr)

immutable CEBinaryOp <: CEBoolean
    op :: AbstractString
    lhs
    rhs
end
import Base.<
<(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp("<", lhs, rhs)
<(lhs :: CEVal, rhs) = CEBinaryOp("<", lhs, value_or_literal(rhs))
<(lhs, rhs :: CEVal) = CEBinaryOp("<", value_or_literal(lhs), rhs)

import Base.>
>(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp(">", lhs, rhs)
>(lhs :: CEVal, rhs) = CEBinaryOp(">", lhs, value_or_literal(rhs))
>(lhs, rhs :: CEVal) = CEBinaryOp(">", value_or_literal(lhs), rhs)

import Base.<=
<=(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp("<=", lhs, rhs)
<=(lhs :: CEVal, rhs) = CEBinaryOp("<=", lhs, value_or_literal(rhs))
<=(lhs, rhs :: CEVal) = CEBinaryOp("<=", value_or_literal(lhs), rhs)

import Base.>=
>=(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp(">=", lhs, rhs)
>=(lhs :: CEVal, rhs) = CEBinaryOp(">=", lhs, value_or_literal(rhs))
>=(lhs, rhs :: CEVal) = CEBinaryOp(">=", value_or_literal(lhs), rhs)

import Base.==
==(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp("=", lhs, rhs)
==(lhs :: CEVal, rhs :: WeakRef) = CEBinaryOp("=", lhs, value_or_literal(rhs.value))
==(lhs :: CEVal, rhs) = CEBinaryOp("=", lhs, value_or_literal(rhs))
==(lhs :: WeakRef, rhs :: CEVal) = CEBinaryOp("=", value_or_literal(lhs.value), rhs)
==(lhs, rhs :: CEVal) = CEBinaryOp("=", value_or_literal(lhs), rhs)

import Base.!=
!=(lhs :: CEVal, rhs :: CEVal) = CEBinaryOp("<>", lhs, rhs)
!=(lhs :: CEVal, rhs) = CEBinaryOp("<>", lhs, value_or_literal(rhs))
!=(lhs, rhs :: CEVal) = CEBinaryOp("<>", value_or_literal(lhs), rhs)

and(lhs :: CETrue, rhs :: CETrue) = lhs
and(lhs :: CEBoolean, rhs :: CETrue) = lhs
and(lhs :: CETrue, rhs :: CEBoolean) = rhs
and(lhs :: CEBoolean, rhs :: CEBoolean) = CEBinaryOp("AND", lhs, rhs)
and(lhs :: DynamoReference, rhs :: CEBoolean) = CEBinaryOp("AND", lhs, rhs)
and(lhs :: CEBoolean, rhs :: DynamoReference) = CEBinaryOp("AND", lhs, rhs)
and(lhs :: DynamoReference, rhs :: DynamoReference) = CEBinaryOp("AND", lhs, rhs)

or(lhs :: CETrue, rhs :: CETrue) = lhs
or(lhs :: CETrue, rhs :: CEBoolean) = lhs
or(lhs :: CEBoolean, rhs :: CETrue) = rhs
or(lhs :: CETrue, rhs :: DynamoReference) = lhs
or(lhs :: DynamoReference, rhs :: CETrue) = rhs
or(lhs :: CEBoolean, rhs :: CEBoolean) = CEBinaryOp("OR", lhs, rhs)
or(lhs :: DynamoReference, rhs :: CEBoolean) = CEBinaryOp("OR", lhs, rhs)
or(lhs :: CEBoolean, rhs :: DynamoReference) = CEBinaryOp("OR", lhs, rhs)
or(lhs :: DynamoReference, rhs :: DynamoReference) = CEBinaryOp("OR", lhs, rhs)

immutable CENot <: CEBoolean
    exp
end

import Base.!
!(exp :: CEBoolean) = CENot(exp)
!(exp :: DynamoAttribute) = CENot(exp)
not(exp :: CEBoolean) = CENot(exp)
not(exp :: DynamoAttribute) = CENot(exp)

immutable CEBetween <: CEBoolean
    attr :: DynamoAttribute
    min
    max
end
between(attr :: DynamoAttribute, min, max) =
    CEBetween(attr, value_or_literal(min), value_or_literal(max))




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

immutable DefaultValue <: DUFnVal
    attr :: DynamoReference
    val
end
get_or_else(attr :: DynamoReference, val) = DefaultValue(attr, value_or_literal(val))

immutable ListAppend <: DynamoUpdateExpression
    attr :: DynamoReference
    val
end
append_to_list(attr :: DynamoReference, val) = ListAppend(attr, value_or_literal(val))

immutable AssignExpression <: DynamoUpdateExpression
    attr :: DynamoReference
    val
end
assign(attr :: DynamoReference, val) = AssignExpression(attr, value_or_literal(val))

immutable SetAddExpression <: DynamoUpdateExpression
    attr :: DynamoReference
    val
end
add_to_set(attr :: DynamoAttribute, val) = SetAddExpression(attr, value_or_literal(val))

immutable SetRemoveExpression <: DynamoUpdateExpression
    attr :: DynamoReference
    val
end
remove_from_set(attr :: DynamoReference, val) = SetRemoveExpression(attr, value_or_literal(val))

immutable DeleteExpression <: DynamoUpdateExpression
    attr :: DynamoReference
end
delete(attr :: DynamoReference) = DeleteExpression(attr)








# Dictionary of references, used to keep the serialization orderly and
# compact.
type DynamoAttrAndValReferences
    gensym_n :: Int64

    attrs
    attrs_reversed # used to avoid double-sending duplicate values

    vals
    vals_reversed # used to avoid double-sending duplicate values
end

type UpdateWriter
    refs :: DynamoAttrAndValReferences

    sets :: Array{Any}
    removes :: Array{Any}
    adds :: Array{Any}
    deletes :: Array{Any}
end

function incr_gensym(w :: DynamoAttrAndValReferences)
    n = w.gensym_n
    w.gensym_n += 1
    n
end

refs_tracker() = DynamoAttrAndValReferences(1, Dict{AbstractString, Any}(), Dict{Any, AbstractString}(),
                                            Dict{AbstractString, Any}(), Dict{Any, AbstractString}())


can_write_expression(::Void) = false
can_write_expression(::CETrue) = false
can_write_expression(x) = true

# Okay, with that out of the way, let's get serializin'
write_expression(w :: DynamoAttrAndValReferences, e :: CESize) =
    "size($(write_expression(w, e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBeginsWith) =
    "begins_with($(write_expression(w, e.attr)), $(write_expression(w, e.val)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEContains) =
    "contains($(write_expression(w, e.attr)), $(write_expression(w, e.val)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEAttributeType) =
    "attribute_type($(write_expression(w, e.attr)), \"$(e.ty)\")"
write_expression(w :: DynamoAttrAndValReferences, e :: CENotExists) =
    "attribute_not_exists($(write_expression(w, e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEExists) =
    "attribute_exists($(write_expression(w, e.attr)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBinaryOp) =
    "($(write_expression(w, e.lhs))) $(e.op) ($(write_expression(w, e.rhs)))"
write_expression(w :: DynamoAttrAndValReferences, e :: CEBetween) =
    "$(write_expression(w, e.attr)) BETWEEN $(write_expression(w, e.min)) AND $(write_expression(w, e.max))"
write_expression(w :: DynamoAttrAndValReferences, e :: CENot) =
    "NOT ($(write_expression(w, e.exp)))"

serialize_expression(expr :: CEBoolean, refs = refs_tracker()) =
    write_expression(refs, expr)




write_expression(w :: DynamoAttrAndValReferences, v :: DefaultValue) =
    "if_not_exists($(write_expression(w, v.attr)), $(write_expression(w, v.val)))"
write_update(w :: UpdateWriter, v :: ListAppend) =
    push!(w.sets, "$(write_expression(w.refs, v.attr)) = list_append($(write_expression(w.refs, v.attr)), $(write_expression(w.refs, v.val)))")
write_update(w :: UpdateWriter, v :: AssignExpression) =
    push!(w.sets, "$(write_expression(w.refs, v.attr)) = $(write_expression(w.refs, v.val))")
write_update(w :: UpdateWriter, v :: SetAddExpression) =
    push!(w.adds, "$(write_expression(w.refs, v.attr)) $(write_expression(w.refs, v.val))")
write_update(w :: UpdateWriter, v :: SetRemoveExpression) =
    push!(w.deletes, "$(write_expression(w.refs, v.attr)) $(write_expression(w.refs, v.val))")
write_update(w :: UpdateWriter, v :: DeleteExpression) =
    push!(w.removes, "$(write_expression(w.refs, v.attr))")

function serialize_updates(arr :: Array, refs = refs_tracker())
    w = UpdateWriter(refs, [], [], [], [])
    for e=arr
        write_update(w, e)
    end

    parts = []
    if w.sets != []
        push!(parts, string("SET ", join(w.sets, ", ")))
    end
    if w.removes != []
        push!(parts, string("REMOVE ", join(w.removes, ", ")))
    end
    if w.adds != []
        push!(parts, string("ADD ", join(w.adds, ", ")))
    end
    if w.deletes != []
        push!(parts, string("DELETE ", join(w.deletes, ", ")))
    end

    join(parts, " ")
end



write_expression(w :: DynamoAttrAndValReferences, nested :: NestedDynamoAttribute) =
    join([write_expression(w, e) for e=nested.attrs], ".")

write_expression(w :: DynamoAttrAndValReferences, v :: DynamoListElement) =
    "$(write_expression(w, v.attr))[$(v.idx)]"

function write_expression(w :: DynamoAttrAndValReferences, val :: DynamoLiteralValue)
    if haskey(w.vals_reversed, val.val)
        return w.vals_reversed[val.val]
    end

    name = ":$(incr_gensym(w))"
    w.vals[name] = null_or_val(val.val)
    w.vals_reversed[val.val] = name
    name
end

function write_expression(w :: DynamoAttrAndValReferences, attr :: DynamoAttribute)
    if haskey(w.attrs_reversed, attr.name)
        return w.attrs_reversed[attr.name]
    end

    name = "#$(incr_gensym(w))"
    w.attrs[name] = attr.name
    w.attrs_reversed[attr.name] = name
    name
end

#  _____ _____ ____ _____ ____        ____        ____  ___   ___  ____
# |_   _| ____/ ___|_   _/ ___|      |  _ \      / ___|/ _ \ / _ \|  _ \
#   | | |  _| \___ \ | | \___ \ _____| |_) |____| |  _| | | | | | | | | |
#   | | | |___ ___) || |  ___) |_____|  _ <_____| |_| | |_| | |_| | |_| |
#   |_| |_____|____/ |_| |____/      |_| \_\     \____|\___/ \___/|____/

include("../src/dynamo_dsl.jl")

# base attribute reference types
@test attr("foo").name == "foo"
@test attr(:foo).name == "foo"

@test [e.name for e=attr("foo", "bar", "baz").attrs] == ["foo", "bar", "baz"]
@test [e.name for e=attr(:foo, :bar, :baz).attrs] == ["foo", "bar", "baz"]

@test attr(:foo)[1].attr.name == "foo"
@test attr(:foo)[1].idx == 1





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

function check_expression(expr, expected; attrs = Dict(), vals = Dict())
    refs = refs_tracker()
    @test expected == write_expression(refs, expr)

    @test refs.attrs == attrs
    @test refs.vals == vals
end

# simple expressions
check_expression(attr("foo"), "#1";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(attr("foo")[1], "#1[1]";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(attr("foo", "bar")[1], "#1.#2[1]";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(value_or_literal(1), ":1";
                 attrs = Dict(), vals = Dict(":1" => Dict("N" => 1)))

check_expression(size(attr("foo")), "size(#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())

check_expression(begins_with(attr("foo"), "bar"), "begins_with(#1, :2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("S" => "bar")))
check_expression(begins_with(attr("foo"), attr("bar")), "begins_with(#1, #2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(contains(attr("foo"), "bar"), "contains(#1, :2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("S" => "bar")))
check_expression(contains(attr("foo"), attr("bar")), "contains(#1, #2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(is_string(attr("foo")), "attribute_type(#1, \"S\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_string_set(attr("foo")), "attribute_type(#1, \"SS\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_real(attr("foo")), "attribute_type(#1, \"N\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_real_set(attr("foo")), "attribute_type(#1, \"NS\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_binary(attr("foo")), "attribute_type(#1, \"B\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_binary_set(attr("foo")), "attribute_type(#1, \"BS\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_bool(attr("foo")), "attribute_type(#1, \"BOOL\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_null(attr("foo")), "attribute_type(#1, \"NULL\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_list(attr("foo")), "attribute_type(#1, \"L\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_map(attr("foo")), "attribute_type(#1, \"M\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(is_document(attr("foo")), "attribute_type(#1, \"M\")";
                 attrs = Dict("#1" => "foo"), vals = Dict())

check_expression(exists(attr("foo")), "attribute_exists(#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(not_exists(attr("foo")), "attribute_not_exists(#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())

check_expression(attr("foo") < 1, "(#1) < (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 < attr("foo"), "(:1) < (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") < attr("bar"), "(#1) < (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(attr("foo") > 1, "(#1) > (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 > attr("foo"), "(:1) > (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") > attr("bar"), "(#1) > (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(attr("foo") <= 1, "(#1) <= (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 <= attr("foo"), "(:1) <= (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") <= attr("bar"), "(#1) <= (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(attr("foo") >= 1, "(#1) >= (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 >= attr("foo"), "(:1) >= (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") >= attr("bar"), "(#1) >= (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(attr("foo") == 1, "(#1) = (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 == attr("foo"), "(:1) = (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") == attr("bar"), "(#1) = (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(attr("foo") != 1, "(#1) <> (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(1 != attr("foo"), "(:1) <> (#2)";
                 attrs = Dict("#2" => "foo"), vals = Dict(":1" => Dict("N" => 1)))
check_expression(attr("foo") != attr("bar"), "(#1) <> (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(and(attr("foo"), attr("bar")), "(#1) AND (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(or(attr("foo"), attr("bar")), "(#1) OR (#2)";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_expression(!attr("foo"), "NOT (#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())
check_expression(not(attr("foo")), "NOT (#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())

check_expression(between(attr("foo"), 1, 2), "#1 BETWEEN :2 AND :3";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1), ":3" => Dict("N" => 2)))
check_expression(between(attr("foo"), attr("bar"), 2), "#1 BETWEEN #2 AND :3";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict(":3" => Dict("N" => 2)))
check_expression(between(attr("foo"), 1, attr("bar")), "#1 BETWEEN :2 AND #3";
                 attrs = Dict("#1" => "foo", "#3" => "bar"), vals = Dict(":2" => Dict("N" => 1)))
check_expression(between(attr("foo"), attr("bar"), attr("baz")), "#1 BETWEEN #2 AND #3";
                 attrs = Dict("#1" => "foo", "#2" => "bar", "#3" => "baz"), vals = Dict())



# compound expressions
check_expression(and(attr("foo"), size(attr("bar")) > 4), "(#1) AND ((size(#2)) > (:3))";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict(":3" => Dict("N" => 4)))
check_expression(and(size(attr("bar")) > 4, attr("foo")), "((size(#1)) > (:2)) AND (#3)";
                 attrs = Dict("#3" => "foo", "#1" => "bar"), vals = Dict(":2" => Dict("N" => 4)))
check_expression(or(attr("foo"), size(attr("bar")) > 4), "(#1) OR ((size(#2)) > (:3))";
                 attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict(":3" => Dict("N" => 4)))
check_expression(or(size(attr("bar")) > 4, attr("foo")), "((size(#1)) > (:2)) OR (#3)";
                 attrs = Dict("#3" => "foo", "#1" => "bar"), vals = Dict(":2" => Dict("N" => 4)))

# duplicate references are combined into one
check_expression(attr("foo") > attr("foo"), "(#1) > (#1)";
                 attrs = Dict("#1" => "foo"), vals = Dict())

# duplicate values are combined into one
check_expression(and(attr("foo") >= 1, attr("foo") <= 1), "((#1) >= (:2)) AND ((#1) <= (:2))";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))

# non-conditions are removed by and
check_expression(and(no_conditions(), attr("foo") != 1), "(#1) <> (:2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))




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


function check_updates(expr :: Array, expected;
                       attrs = Dict(), vals = Dict())
    refs = refs_tracker()
    @test expected == serialize_updates(expr, refs)

    @test refs.attrs == attrs
    @test refs.vals == vals
end
check_updates(x :: DynamoUpdateExpression, expected; attrs = Dict(), vals = Dict()) =
    check_updates([x], expected; attrs=attrs, vals=vals)


# maybe should be above -- this function is only for update expressions though
check_expression(get_or_else(attr("foo"), 1), "if_not_exists(#1, :2)";
                 attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 1)))

# lone operations
check_updates(append_to_list(attr("foo"), 3), "SET #1 = list_append(#1, :2)";
              attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 3)))
check_updates(append_to_list(attr("foo"), attr("bar")), "SET #1 = list_append(#1, #2)",
              attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_updates(assign(attr("foo"), 3), "SET #1 = :2";
              attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 3)))
check_updates(assign(attr("foo"), attr("bar")), "SET #1 = #2";
              attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_updates(add_to_set(attr("foo"), 3), "ADD #1 :2";
              attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 3)))
check_updates(add_to_set(attr("foo"), attr("bar")), "ADD #1 #2";
              attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_updates(remove_from_set(attr("foo"), 3), "DELETE #1 :2";
              attrs = Dict("#1" => "foo"), vals = Dict(":2" => Dict("N" => 3)))
check_updates(remove_from_set(attr("foo"), attr("bar")), "DELETE #1 #2";
              attrs = Dict("#1" => "foo", "#2" => "bar"), vals = Dict())

check_updates(delete(attr("foo")), "REMOVE #1";
              attrs = Dict("#1" => "foo"), vals = Dict())

# combo time!
check_updates([delete(attr("foo")), append_to_list(attr("bar"), 7), delete(attr("baz")), assign(attr("boo"), 11)],
              "SET #2 = list_append(#2, :3), #5 = :6 REMOVE #1, #4";
              attrs = Dict("#1" => "foo", "#2" => "bar", "#4" => "baz", "#5" => "boo"),
              vals = Dict(":3" => Dict("N" => 7), ":6" => Dict("N" => 11)))

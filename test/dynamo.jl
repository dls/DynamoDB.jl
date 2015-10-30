#  _____ _____ ____ _____ ____        ____        ____  ___   ___  ____
# |_   _| ____/ ___|_   _/ ___|      |  _ \      / ___|/ _ \ / _ \|  _ \
#   | | |  _| \___ \ | | \___ \ _____| |_) |____| |  _| | | | | | | | | |
#   | | | |___ ___) || |  ___) |_____|  _ <_____| |_| | |_| | |_| | |_| |
#   |_| |_____|____/ |_| |____/      |_| \_\     \____|\___/ \___/|____/


# NOTE TO READERS:
# This is a test case, so we'll be exercising advanced features
# provided by DynamoDB. If you're looking for simple examples to start
# from, please check out example.jl in the doc/ folder of this repo.


# see runtests.jl for Foo's definition

foo_basic = dynamo_table(Foo, "foo_basic", :a)
foo_basic_lsi = dynamo_local_index(foo_basic, "foo_basic_indexed_on_b", :b)
foo_basic_gsi = dynamo_global_index(foo_basic, "foo_basic_global_index_on_b_a", :b, :a)
foo_basic_gsi_no_idx = dynamo_global_index(foo_basic, "foo_basic_global_index_on_b", :b)

foo_range = dynamo_table(Foo, "foo_range", :a, :b)
foo_range_gsi = dynamo_global_index(foo_range, "foo_range_global_index_on_b_a", :b, :a)


# keydict in all its glory
@test_throws ErrorException DynamoDB.keydict(foo_basic, 1, 2)
@test DynamoDB.keydict(foo_basic, 1) == Dict("a" => Dict("N" => "1"))

@test DynamoDB.keydict(foo_basic_lsi, 1, 2) == Dict("a" => Dict("N" => "1"), "b" => Dict("N" => "2"))
@test_throws ErrorException DynamoDB.keydict(foo_basic_lsi, 1)

@test DynamoDB.keydict(foo_basic_gsi, 1, 2) == Dict("b" => Dict("N" => "1"), "a" => Dict("N" => "2"))
@test DynamoDB.keydict(foo_basic_gsi, 1) == Dict("b" => Dict("N" => "1"), "a" => Dict("NULL" => true))

@test_throws ErrorException DynamoDB.keydict(foo_basic_gsi_no_idx, 1, 2)
@test DynamoDB.keydict(foo_basic_gsi, 1) == Dict("b" => Dict("N" => "1"), "a" => Dict("NULL" => true))


@test DynamoDB.keydict(foo_range, 1, 2) == Dict("a" => Dict("N" => "1"), "b" => Dict("N" => "2"))
@test DynamoDB.keydict(foo_range, 1) == Dict("a" => Dict("N" => "1"), "b" => Dict("NULL" => true))

@test DynamoDB.keydict(foo_range_gsi, 1, 2) == Dict("b" => Dict("N" => "1"), "a" => Dict("N" => "2"))
@test DynamoDB.keydict(foo_range_gsi, 1) == Dict("b" => Dict("N" => "1"), "a" => Dict("NULL" => true))


@test_throws ErrorException DynamoDB.check_status(404, "test")
@test DynamoDB.check_status(200, "test") == nothing

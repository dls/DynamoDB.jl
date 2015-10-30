#  _____ _____ ____ _____ ____        ____        ____  ___   ___  ____
# |_   _| ____/ ___|_   _/ ___|      |  _ \      / ___|/ _ \ / _ \|  _ \
#   | | |  _| \___ \ | | \___ \ _____| |_) |____| |  _| | | | | | | | | |
#   | | | |___ ___) || |  ___) |_____|  _ <_____| |_| | |_| | |_| | |_| |
#   |_| |_____|____/ |_| |____/      |_| \_\     \____|\___/ \___/|____/


# NOTE TO READERS:
# This is a test case, so we'll be exercising advanced features
# provided by DynamoDB. If you're looking for simple examples to start
# from, please check out example.jl in the doc/ folder of this repo.


include("../src/dynamo.jl")

# see runtests.jl for Foo's definition

foo_basic = dynamo_table(Foo, "foo_basic", :a, nothing)
foo_basic_lsi = dynamo_local_index(foo_basic, "foo_basic_indexed_on_b", :b)
foo_basic_gsi = dynamo_global_index(foo_basic, "foo_basic_global_index_on_b_a", :b, :a)

foo_range = dynamo_table(Foo, "foo_range", :a, :b)
foo_range_gsi = dynamo_global_index(foo_range, "foo_range_global_index_on_b_a", :b, :a)

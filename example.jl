type Foo
    a
    b
    c

    version
    txn
end
Foo(a, b, c) = Foo(a, b, c, nothing, nothing)

FooTable = DynamoTable(Foo, string_key("a"), real_key("b"),
                       reads=10, writes=10,
                       version_column="version", txn_column="txn")
FooLSI = DynamoLocalIndex(FooTable, real_key("c"), reads=10, writes=10)
FooGSI = DynamoGlobalIndex(FooTable, real_key("c"), reads=10, writes=10)
create_table(FooTable, FooLSI, FooGSI)

load_foo(args...) = get_item(FooTable, args...)
load_foo_lsi(args...) = get_item(FooLSI, args...)
load_foos_by_c(args...) = get_items(FooGSI, args...)
persist(f :: Foo, args...) = put_item(FooTable, f, args...)


persist(Foo("one", 2, 3))

load_foo("one", 2)
load_foo_lsi("one", 3)
persist(Foo("one", 2, 3)) # boom ... missing version

persist(Foo("two", 3, 4))

item = load_foo("one", 2)
item.b = 20
persist(item)

dynamo_transaction(retries=3) do |t|
    one = load_foo("one", 2)
    two = load_foo("two", 3)

    # we want an atomic write on both
    one.b += 1
    two.b -= 1

    persist(t, one)
    persist(t, two)

    rollback(t) # undooooo!
    error("blah") # same as rollback

    commit(t) # writes it
end


f = load_foo("one", 2)
f.c += 1
persist(f, (attr("c") >= 10) && size(attr("a")) <= 4)
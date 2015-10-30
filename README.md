# DynamoDB

[![Build Status](https://travis-ci.org/dls/DynamoDB.jl.svg?branch=master)](https://travis-ci.org/dls/DynamoDB.jl)

Pure julia DynamoDB bindings. DynamoDB is a proprietary NoSQL database
offered by amazon which provides virtually unlimited scaling at [very
low cost](https://aws.amazon.com/dynamodb/pricing/).

DynamoDB provides fairly expressive row-level operations (conditional
writes, atomic increment, etc), but absolutely no aggregate
functions... this is the cost of scaling if you will, requiring a
different way of thinking about how you're authoring your application.

Enough talk, time for an example.


```
using DynamoDB

type DynamoExample
     id
     order
     int_to_update
end

const table = dynamo_table(DynamoExample, "JULIA_TESTING", :id, :order; env=env)

put_item(table, DynamoExample("string-based-id", 1, 10))
put_item(table, DynamoExample("string-based-id", 2, 100))
put_item(table, DynamoExample("string-based-id2", 1, 1000))

@show get_item(table, "string-based-id", 1).int_to_update # --> 10

update_item(table, "string-based-id", 1,
            set(attr("int_to_update"), attr("int_to_update") + 1)) # increments int_to_update

@show get_item(table, "string-based-id", 1).int_to_update # --> 11

update_item(table, "string-based-id", 1,
            set(attr("int_to_update"), attr("int_to_update") + 1),
            conditions=attr("int_to_update") == 10) # will fail to increment -- int_to_upate is 11
```





function task_to_array(task)
    arr = []
    for e=task
        push!(arr, e)
    end
    arr
end

# note this is a feature, not a bug (I think)
# AWS supports table (and even row level) permissions via IAM
const env = AWSEnv(;id="AKIAJE6VHSX64EMJUAJA", key="ktpHIUI2vfYSgXStr+NCy0HN8fHgQdw6SgvbhHky")

# could also be done using a julia Type or Immutable if you prefer
const table = dynamo_table(Dict, "JULIA_TESTING", :id, :order; env=env)

# unique key for testing purposes
const id_key = string(Base.Random.uuid4())

# let's load some data
for i=1:25:100
    batch_put_item(table,
                   Dict("id" => id_key, "order" => i + 0),
                   Dict("id" => id_key, "order" => i + 1),
                   Dict("id" => id_key, "order" => i + 2),
                   Dict("id" => id_key, "order" => i + 3),
                   Dict("id" => id_key, "order" => i + 4),
                   Dict("id" => id_key, "order" => i + 5),
                   Dict("id" => id_key, "order" => i + 6),
                   Dict("id" => id_key, "order" => i + 7),
                   Dict("id" => id_key, "order" => i + 8),
                   Dict("id" => id_key, "order" => i + 9),
                   Dict("id" => id_key, "order" => i + 10),
                   Dict("id" => id_key, "order" => i + 11),
                   Dict("id" => id_key, "order" => i + 12),
                   Dict("id" => id_key, "order" => i + 13),
                   Dict("id" => id_key, "order" => i + 14),
                   Dict("id" => id_key, "order" => i + 15),
                   Dict("id" => id_key, "order" => i + 16),
                   Dict("id" => id_key, "order" => i + 17),
                   Dict("id" => id_key, "order" => i + 18),
                   Dict("id" => id_key, "order" => i + 19),
                   Dict("id" => id_key, "order" => i + 20),
                   Dict("id" => id_key, "order" => i + 21),
                   Dict("id" => id_key, "order" => i + 22),
                   Dict("id" => id_key, "order" => i + 23),
                   Dict("id" => id_key, "order" => i + 24))
end

itr = query(table, id_key, between(attr("order"), 14, 16); consistant_read=true)
items = task_to_array(itr)
@test length(items) == 3 # 14, 15, and 16
items[2]["order"] = 200
items[2]["some_new_field"] = "another value we might want"
items[2]["sub_doc"] = Dict("a" => 1, "b" => 2)

put_item(table, items[2]) # writes the value as a *new* item.

itr = query(table, id_key, between(attr("order"), 14, 16); consistant_read=true)
items = task_to_array(itr)
@test length(items) == 3


item = get_item(table, id_key, 200)
@test item["some_new_field"] == "another value we might want"
@test item["sub_doc"] == Dict("a" => 1, "b" => 2)


update_item(table, id_key, 200, assign(attr("sub_doc", "c"), 3))


item = get_item(table, id_key, 200)
@test item["some_new_field"] == "another value we might want"
@test item["sub_doc"] == Dict("a" => 1, "b" => 2, "c" => 3)


delete_item(table, id_key, 200)
@test get_item(table, id_key, 200) == nothing


items = batch_get_item(table, (id_key, 1), (id_key, 2), (id_key, 3), (id_key, 200))
@assert length(items) == 3 # since 200 was deleted


for e=scan(table; limit=10) # look at some random items in the table... without a limit this goes on forever
    @assert e["id"] != nothing
    @assert e["order"] != nothing
end



# cleanup. the projection asks DynamoDB to only return the order attribute to us.
for e = query(table, id_key; projection=[attr("order")])
    delete_item(table, id_key, e["order"])
end



# Pssst... wanna set something like this up for yourself?
# Here's the policy file:

# {
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Effect": "Allow",
#             "Action": [
#                 "dynamodb:GetItem",
#                 "dynamodb:PutItem",
#                 "dynamodb:UpdateItem",
#                 "dynamodb:DeleteItem",
#                 "dynamodb:BatchGetItem",
#                 "dynamodb:BatchWriteItem",
#                 "dynamodb:Query",
#                 "dynamodb:Scan"
#             ],
#             "Resource": [
#                 "arn:aws:dynamodb:us-east-1:300203084218:table/JULIA_TESTING",
#                 "arn:aws:dynamodb:us-east-1:300203084218:table/JULIA_TESTING/index/*"
#             ]
#         }
#     ]
# }

# You can obviously add operations, and tables to suit your needs :)
# ... also note you can also provide row-level permissions using
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/FGAC_DDB.Examples.html
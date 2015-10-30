#  _____ _____ ____ _____ ____        ____        ____  ___   ___  ____
# |_   _| ____/ ___|_   _/ ___|      |  _ \      / ___|/ _ \ / _ \|  _ \
#   | | |  _| \___ \ | | \___ \ _____| |_) |____| |  _| | | | | | | | | |
#   | | | |___ ___) || |  ___) |_____|  _ <_____| |_| | |_| | |_| | |_| |
#   |_| |_____|____/ |_| |____/      |_| \_\     \____|\___/ \___/|____/



include("../src/dynamo_json.jl")

#                _       _ _
#  ___  ___ _ __(_) __ _| (_)_______
# / __|/ _ \ '__| |/ _` | | |_  / _ \
# \__ \  __/ |  | | (_| | | |/ /  __/
# |___/\___|_|  |_|\__,_|_|_/___\___|

@test null_or_val(nothing) == Dict("NULL" => true)

@test null_or_val(true) == Dict("BOOL" => true)
@test null_or_val(false) == Dict("BOOL" => false)

@test null_or_val("foo") == Dict("S" => "foo")

@test null_or_val(21) == Dict("N" => "21")
@test null_or_val(21.1) == Dict("N" => "21.1")

@test null_or_val([1, 2, 3]) == Dict("L" => [Dict("N" => "1"), Dict("N" => "2"), Dict("N" => "3")])

# result lists is (unsurprisingly) unordered...
res = null_or_val(Set([1, 2, 3]))
@test length(keys(res)) == 1
@test length(res["NS"]) == 3
@test contains(==, res["NS"], Dict("N" => "1"))
@test contains(==, res["NS"], Dict("N" => "2"))
@test contains(==, res["NS"], Dict("N" => "3"))

res = null_or_val(Set(["1", "2", "3"]))
@test length(keys(res)) == 1
@test length(res["SS"]) == 3
@test contains(==, res["SS"], Dict("S" => "1"))
@test contains(==, res["SS"], Dict("S" => "2"))
@test contains(==, res["SS"], Dict("S" => "3"))

@test null_or_val(Dict("four" => 3, 7 => 11)) == Dict("M"=>Dict("four"=>Dict("N"=>"3"),"7"=>Dict("N"=>"11")))

# see runtests.jl for Foo's definition

@test null_or_val(Foo(3, "fourty")) == Dict("M"=>Dict("a"=>Dict("N"=>"3"),"b"=>Dict("S"=>"fourty")))



#      _                     _       _ _
#   __| | ___  ___  ___ _ __(_) __ _| (_)_______
#  / _` |/ _ \/ __|/ _ \ '__| |/ _` | | |_  / _ \
# | (_| |  __/\__ \  __/ |  | | (_| | | |/ /  __/
#  \__,_|\___||___/\___|_|  |_|\__,_|_|_/___\___|

function check_round_trip(val)
    @test value_from_attributes(null_or_val(val)) == val
end

# base types
check_round_trip(nothing)
check_round_trip(true)
check_round_trip(false)
check_round_trip("foo")
check_round_trip("false")
check_round_trip("true")
check_round_trip("nothing")
check_round_trip("21")
check_round_trip(21)
check_round_trip(21.0)

# compound types
check_round_trip([1, 2, 3])
check_round_trip(Set([1, 2, 3]))
check_round_trip(Set(["1", "2", "3"]))

@test value_from_attributes(null_or_val(Dict("four" => 3, 7 => 11))) == Dict("four" => 3, "7" => 11)
@test value_from_attributes(null_or_val(Foo(3, "fourty"))) == Dict("a"=>3,"b"=>"fourty")

res = value_from_attributes(Foo, null_or_val(Foo(3, "fourty")))
@test res.a == 3
@test res.b == "fourty"

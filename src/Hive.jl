module Hive

using Thrift

import Base: close

export HiveSession, HiveAuth, HiveAuthSASLPlain
export close, get_info

export InfoType

# package code goes here
include("HS2/HS2.jl")
include("hs2api.jl")

end # module

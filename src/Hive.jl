module Hive

using Thrift
using DataFrames

import Base: close, isready, wait, show

export HiveSession, HiveAuth, HiveAuthSASLPlain
export close, isready, wait, cancel, get_info, catalogs, show
export fetchsize, fetchsize!, fetchfirst, fetchnext, dataframe

export InfoType

# enable logging only during debugging
using Logging
const logger = Logging.configure(level=DEBUG)
#const logger = Logging.configure(filename="/tmp/hive$(getpid()).log", level=DEBUG)
macro logmsg(s)
    quote
        debug($(esc(s)))
    end
end
#macro logmsg(s)
#end

# package code goes here
include("HS2/HS2.jl")
using .HS2
include("util.jl")
include("sess.jl")
include("types.jl")
include("resultset.jl")
include("api.jl")

end # module

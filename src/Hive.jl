__precompile__(true)
module Hive

using Compat
using Thrift
using DataFrames
using DataArrays

import Base: close, isready, show, eof, start, next, done

export HiveSession, HiveAuth, HiveAuthSASLPlain
export eof, close, isready, result, cancel, get_info, show
export catalogs, schemas, tables, tabletypes, functions, columns, execute
export dataframe, dataframes, records, columnchunks, columnchunk
export start, next, done

export InfoType

# enable logging only during debugging
#using Logging
#const logger = Logging.configure(level=DEBUG)
##const logger = Logging.configure(filename="/tmp/hive$(getpid()).log", level=DEBUG)
#macro logmsg(s)
#    quote
#        debug($(esc(s)))
#    end
#end
macro logmsg(s)
end

# package code goes here
include("HS2/HS2.jl")
using .HS2
include("sess.jl")
include("types.jl")
include("resultset.jl")
include("util.jl")
include("api.jl")

end # module

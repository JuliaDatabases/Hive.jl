module Hive

using Thrift
using Markdown
using Dates
using Unicode

import Base: close, isready, show, eof, iterate, vcat

export HiveSession, HiveAuth, HiveAuthSASLPlain, Tabular
export eof, close, isready, result, cancel, get_info, show
export catalogs, schemas, tables, tabletypes, functions, columns, execute
export tabular, tabulars, records, columnchunks, columnchunk
export iterate, iteratorsize

export InfoType

# package code goes here
include("HS2/HS2.jl")
using .HS2
include("sess.jl")
include("types.jl")
include("resultset.jl")
include("util.jl")
include("api.jl")

end # module

using Hive
using Base.Test
using DataFrames

function open_database(f::Function)
    session = HiveSession()
    try
        result = execute(session, "use default")
        @test round(Int, result) == 0
        f(session)
    finally
        close(session)
    end
end

function fetch_server_metadata(session)
    server_name = get_info(session, InfoType.CLI_SERVER_NAME)
    @test length(server_name) > 0
    println("Server name: $server_name")

    cats = catalogs(session)
    @test isa(cats, DataFrame)
    println("Catalogs:")
    println(cats)

    sch = schemas(session)
    # at least the default schema should be present
    @test prod(size(sch)) > 1
    println("Schemas:")
    println(sch)

    tbls = tables(session)
    @test isa(tbls, DataFrame)
    println("Tables:")
    println(tbls)

    ttypes = tabletypes(session)
    @test isa(ttypes, DataFrame)
    println("Table types:")
    println(ttypes)

    cols = columns(session)
    @test isa(cols, DataFrame)
    println("Columns:")
    println(cols)

    fns = functions(session, "%")
    # at least the default functions should be present
    @test prod(size(fns)) > 1
    println("Functions:")
    println(fns)
    nothing
end

function create_table_twitter_small(session)
    rs = execute(session, "show tables like 'twitter_small'")
    table_exists = prod(size(dataframe(rs))) > 0

    if table_exists
        println("Use existing table: twitter_small")
    else
        println("Create table: twitter_small")
        result = execute(session, "create table twitter_small (fromid int, toid int) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
        @test round(Int, result) == 0.0
    end

    rs = execute(session, "select count(*) from twitter_small")
    rowcount = dataframe(rs)[1,1]
    if rowcount > 0
        println("Use existing data: $rowcount rows")
    else
        rowcount = 10^4
        println("Insert data: $rowcount rows")
        filename = "/tmp/twitter_small"
        open(filename, "w") do io
            writedlm(io, convert(Array{Int}, rand(UInt16, rowcount, 2)), ',')
        end
        result = execute(session, "load data local inpath '$filename' into table twitter_small")
        @test round(Int, result) == 0.0
    end
end

function create_table_datatype_test(session)
    rs = execute(session, "show tables like 'datatype_test'")
    table_exists = prod(size(dataframe(rs))) > 0
    cols = (
            ("tbool"    , "boolean"     , ()->rand(Bool)),
            ("tint8"    , "tinyint"     , ()->rand(Int8)),
            ("tint16"   , "smallint"    , ()->rand(Int16)),
            ("tint32"   , "int"         , ()->rand(Int32)),
            ("tint64"   , "bigint"      , ()->rand(Int64)),
            ("tfloat32" , "float"       , ()->rand(Float32)),
            ("tfloat64" , "double"      , ()->rand(Float64)),
            ("tstr"     , "string"      , ()->randstring()),
            ("tdatetime", "timestamp"   , ()->replace(string(now() - Dates.Day(rand(UInt8))), "T", " ")),
            ("tbigfloat", "decimal"     , ()->rand(Float64)),
            ("tdate"    , "date"        , ()->Date(now() - Dates.Day(rand(UInt8)))),
            ("tchar"    , "char(1)"     , ()->('A' + rand(1:20)))
    )

    if table_exists
        println("Use existing table: datatype_test")
    else
        println("Create table: datatype_test")
        ct = join(["$(x[1]) $(x[2])" for x in cols], ", ")
        result = execute(session, "create table datatype_test ($ct) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
        @test round(Int, result) == 0.0
    end

    rs = execute(session, "select count(*) from datatype_test")
    rowcount = dataframe(rs)[1,1]
    if rowcount > 0
        println("Use existing data: $rowcount rows")
    else
        rowcount = 10^4
        println("Insert data: $rowcount rows")
        filename = "/tmp/datatype_test"
        colvals = hcat([Any[fn() for idx in 1:rowcount] for fn in [x[3] for x in cols]]...)
        open(filename, "w") do io
            writedlm(io, colvals, ',')
        end
        result = execute(session, "load data local inpath '$filename' into table datatype_test")
        @test round(Int, result) == 0.0
    end
end

function fetch_records(session)
    rs = execute(session, "select min(fromid), max(fromid) from twitter_small")
    minmax = dataframe(rs)
    maxval = ceil(Int, mean([minmax[1,1], minmax[1,2]]))
    lim = 10000

    println("Execute, record iterator:")
    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim")
    cnt = 0
    for rec in records(rs)
        if rec !== nothing
            (cnt <= 100) && println(rec)
            (cnt == 100) && println("...")
            cnt += 1
        end
    end
    close(rs)
    @test cnt <= lim

    println("Execute, dataframe iterator:")
    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim")
    cnt = 0
    for frames in dataframes(rs)
        println(frames)
        cnt += size(frames, 1)
    end
    close(rs)
    @test cnt <= lim
 
    println("Execute, column chunk iterator:")
    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim")
    cnt = 0
    for colframe in columnchunks(rs)
        for cols in colframe
            println("name  : ", cols[1])
            println("values: ", cols[2][1:min(length(cols[2]), 10)])
            @test typeof(cols[2]) == Vector{Int32}
        end
        cnt += length(colframe[1][2])
    end
    close(rs)
    @test cnt <= lim

    println("Execute, async:")
    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim"; async=true)
    while !isready(rs)
        println("waiting...")
        sleep(10)
    end
    rs = result(rs)
    df = dataframe(rs)
    @test size(df, 1) <= lim
    println(df)

    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim"; async=true)
    while !isready(rs)
        println("waiting...")
        sleep(10)
    end
    rs = result(rs)
    cc = columnchunk(rs)
    @test size(cc[1][2], 1) <= lim
    @test typeof(cc[1][2]) == Vector{Int32}
    @test typeof(cc[2][2]) == Vector{Int32}
    println([(n=>(length(v), typeof(v))) for (n,v) in cc])

    println("Execute, datatypes:")
    rs = execute(session, "select * from datatype_test")
    cols = columnchunk(rs, 100)
    coltypes = [Bool, Int8, Int16, Int32, Int64, Float32, Float64, String, Union{NAtype,DateTime}, Union{NAtype,BigFloat}, Union{NAtype,Date}, String]
    for ((cn,cv),ct) in zip(cols, coltypes)
        println(cn, " => ", cv[1:min(length(cv),10)])
        @test typeof(cv) == Vector{ct}
        @test length(cv) == 10^4
    end
    close(rs)

    nothing
end

open_database() do session
    create_table_twitter_small(session)
    create_table_datatype_test(session)
    fetch_server_metadata(session)
    fetch_records(session)
end

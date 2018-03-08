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

function create_table(session)
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

function fetch_records(session)
    rs = execute(session, "select min(fromid), max(fromid) from twitter_small")
    minmax = dataframe(rs)
    maxval = ceil(Int, mean([minmax[1,1], minmax[1,2]]))
    lim = 10000

    println("Execute, record iterator:")
    rs = execute(session, "select * from twitter_small where fromid <= $maxval limit $lim")
    cnt = 0
    for rec in records(rs)
        println(rec)
        cnt += 1
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
            println("values: ", cols[2])
            @test typeof(cols[2]) == Vector{Int32}
        end
        cnt += size(colframe, 1)
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
    println(cc)
    nothing
end

open_database() do session
    create_table(session)
    fetch_server_metadata(session)
    fetch_records(session)
end

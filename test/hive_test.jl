using Hive
using Base.Test

session = HiveSession()

server_name = get_info(session, InfoType.CLI_SERVER_NAME)
println("Server name: $server_name")

println("Catalogs:")
println(catalogs(session))

println("Schemas:")
println(schemas(session))

println("Tables:")
println(tables(session))

println("Table types:")
println(tabletypes(session))

println("Columns:")
println(columns(session))

println("Functions:")
println(functions(session, "%"))

println("Execute, record iterator:")
rs = execute(session, "select * from twitter_small where fromid < 100")
for rec in records(rs)
   println(rec)
end
close(rs)

println("Execute, dataframe iterator:")
rs = execute(session, "select * from twitter_small where fromid < 100")
for frames in dataframes(rs)
   println(frames)
end
close(rs)

println("Execute, async:")
rs = execute(session, "select * from twitter_small where fromid < 100"; async=true)
while !isready(rs)
    println("waiting...")
    sleep(10)
end
rs = result(rs)
println(dataframe(rs))

close(session)

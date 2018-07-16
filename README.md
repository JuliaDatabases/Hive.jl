# Hive.jl

A client for distributed SQL engines that provide a HiveServer2 interface.
E.g.: [Hive](https://hive.apache.org/), [Spark SQL](http://spark.apache.org/sql/), [Impala](http://impala.io/)

[![Build Status](https://travis-ci.org/JuliaDatabases/Hive.jl.svg?branch=master)](https://travis-ci.org/JuliaDatabases/Hive.jl)

## Connecting

To connect to the server, create an instance of HiveSession.

````
session = HiveSession()
````

Without any parameters, this attempts to connect to a server running on `localhost` port `10000`.
A remote server can be connected to by specifying the hostname and port number.

````
session = HiveSession("localhost", 10000)
````

As of now only SASL-Plain authentication is supported, without any `qop`. The default implementation
authenticates with the same user-id as that of the login shell. That can be overridden by providing
an appropriate instance of `HiveAuth`.

````
session = HiveSession("localhost", 10000, HiveAuthSASLPlain("uid", "pwd", "zid"))
````

The thrift `TBinaryProtocol` is used by default, which is also the default for most server setups.
Other protocols can be used by specifying the optional named parameter `tprotocol`.
As of now, `:binary` and `:compact` protocols are supported.

````
session = HiveSession("localhost", 10000; tprotocol=:binary)
````

## Executing Queries

Statement to be executed can be DML, DDL, SET, etc.

Optional `config` parameter can have additional keyword parameters that will be passed as configuration 
properties that are overlayed on top of the the existing session configuration before this statement is
executed. They apply to this statement only and are not permanent.

When `async` is `true`, execution is asynchronous and a `PendingResult` may be returned.
If the returned value is a `PendingResult`:

- `isready` must be called on the `PendingResult` instance to check for completion.
- once ready, calling `result` on it returns `ResultSet`
- when not ready, calling `result` returns the same `PendingResult` instance

````
rs = execute(session, "select * from twitter_small where fromid < 100";
             async=true, config=Dict())
while !isready(rs)
    println("waiting...") 
    sleep(10)
end
rs = result(rs)
````

## Working with a Result Set

Result sets can be iterated upon with iterators and must be closed at the end by calling the `close` method, to release resources.

Two kinds of iterators are available as of now:
- **record iterator**: returns a row at a time as a `Tuple`.
- **column chunk iterator**: returns a list of column name and column data pairs for a block of records

Calling `records` results in a record iterator:

````
rs = execute(session, "select * from twitter_small where fromid < 100")
for rec in records(rs)
   println(rec)
end
close(rs)
````

Calling `columnchunks` results in a column chunk iterator:

```
rs = execute(session, "select * from twitter_small where fromid < 100")
for colframe in columnchunks(rs)
    for cols in colframe
        println("name  : ", cols[1])
        println("values: ", cols[2])
    end
end
close(rs)
````

All records can be read from a result set by simply calling `columnchunk`. This should only be used when the result is sure to fit in memory.

````
rs = execute(session, "select * from twitter_small where fromid < 100")
println(columnchunk(rs))
close(rs)
````

## Pretty Printing Data

An exported type named `Tabular` can be used to display column chunks in a tabular format. Optional display parameters can be passed:
- `full`: whether to display all rows (default is `false`, i.e. to skip data from the middle to fit display height)
- `compact_if_too_wide`: whether to display only column summary if display width is not enough to fit data (default is `true`)

```
rs = execute(session, "select * from twitter_small where fromid < 100")
Tabular(columnchunk(rs); full=false, compact_if_too_wide=true)
```

A convenience iterator that produces `Tabular` types is also provided:

```
rs = execute(session, "select * from twitter_small where fromid < 100")
for tab in tabulars(rs)
    println(tab)
end
```

## Fetching Server/Table Metadata

Server configuration can be fetched by calling `get_info`.
Here, `info_type` is one of the values from the enumeration `InfoType`, e.g. `InfoType.CLI_SERVER_NAME`.

````
info_type = InfoType.CLI_SERVER_NAME
info = get_info(session, info_type)
````

Catalogs, Schemas, TableTypes, Functions, Tables, Columns defined on the server can be listed by calling the appropriate API listed below.
The results are returned as a DataFrame.

````
# list all catalogs
catalogs(session)

# list all table types configured
tabletypes(session)

# list all schemas
schemas(session)

# schema list can be optionally filtered with catalog and schema name
schemas(session; catalog_pattern="%", schema_pattern="%")

# list all tables
tables(session)

# table list can be optionally filtered
tables(session; catalog_pattern="%", schema_pattern="%",
       table_pattern="%", table_types=[])

# list columns
columns(session)

# columns list can be optionally filtered
columns(session; catalog="", schema_pattern="%",
        table_pattern="%", column_pattern="%")

# list functions matching given function name pattern
functions(session, "%")

# functions list can be optionally filtered
functions(session, "%"; catalog="", schema_pattern="%")
````

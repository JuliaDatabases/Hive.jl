
function response_to_tabular(session::HiveSession, response, fetchsize::Integer; cached_schema::Union{AbstractString,Symbol,Nothing}=nothing)
    rs = response_to_resultset(session, response)::ResultSet
    schema(rs; cached=cached_schema)
    tabular(rs, fetchsize; compact_if_too_wide=false)
end

function response_to_resultset(session::HiveSession, response; force_pending::Bool=false)
    ready = check_status(response.status)
    force_pending && (ready = false)
    result(session, ready, response.operationHandle)
end

"""
A search pattern used in the below methods can have:
- '_': Any single character.
- '%': Any sequence of zero or more characters.
- '\': Escape character used to include special characters,

e.g. '_', '%', '\'. If a '\' precedes a non-special character it has no special meaning and is interpreted literally.
"""
const Pattern = AbstractString

"""
Returns the list of catalogs (databases).
Results are ordered by TABLE_CATALOG.
"""
function catalogs(session::HiveSession; fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn
    request = TGetCatalogsReq(; sessionHandle=conn.handle.sessionHandle)
    response = GetCatalogs(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:catalogs)
end

"""
Retrieves the schema names available in this database.
The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
"""
function schemas(session::HiveSession; catalog_pattern::Pattern="", schema_pattern::Pattern="", fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = TGetSchemasReq(; sessionHandle=conn.handle.sessionHandle)
    isempty(catalog_pattern) || (request.catalogName = catalog_pattern)
    isempty(schema_pattern) || (request.schemaName = schema_pattern)

    response = GetSchemas(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:schemas)
end

"""
Returns a list of tables with catalog, schema, and table type information.
Results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, and TABLE_NAME
"""
function tables(session::HiveSession; catalog_pattern::Pattern="", schema_pattern::Pattern="", table_pattern::Pattern="", table_types::Array=[], fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = TGetTablesReq(; sessionHandle=conn.handle.sessionHandle)
    isempty(catalog_pattern) || (request.catalogName = catalog_pattern)
    isempty(schema_pattern) || (request.schemaName = schema_pattern)
    isempty(table_pattern) || (request.tableName = table_pattern)
    isempty(table_types) || (request.tableTypes = convert(Array{String,1}, table_types))

    response = GetTables(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:tables)
end

"""
Returns the table types available in this database.
The results are ordered by table type.
"""
function tabletypes(session::HiveSession; fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn
    request = TGetTableTypesReq(; sessionHandle=conn.handle.sessionHandle)
    response = GetTableTypes(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:tabletypes)
end

"""
Returns a list of columns in the specified tables.
Optional parameter `catalog` must contain a full catalog name.
Optional parameters `schema_pattern`, `table_pattern` and `column_pattern` can contain a search pattern.
Result Set Columns are the same as those for the ODBC CLIColumns function.
"""
function columns(session::HiveSession; catalog::AbstractString="", schema_pattern::Pattern="", table_pattern::Pattern="", column_pattern::Pattern="", fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = TGetColumnsReq(; sessionHandle=conn.handle.sessionHandle)
    isempty(catalog) || (request.catalogName = catalog)
    isempty(schema_pattern) || (request.schemaName = schema_pattern)
    isempty(table_pattern) || (request.tableName = table_pattern)
    isempty(column_pattern) || (request.columnName = column_pattern)

    response = GetColumns(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:columns)
end

"""
Returns a list of functions supported by the data source.
Catalog name must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search.
Schema name pattern must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search.
Function name pattern must match the function name as it is stored in the database.
The behavior of this function matches `java.sql.DatabaseMetaData.getFunctions()`.
"""
function functions(session::HiveSession, function_pattern::Pattern; catalog::AbstractString="", schema_pattern::Pattern="", fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = TGetFunctionsReq(;sessionHandle=conn.handle.sessionHandle, functionName=function_pattern)
    isempty(catalog) || (request.catalogName = catalog)
    isempty(schema_pattern) || (request.schemaName = schema_pattern)

    response = GetFunctions(conn.client, request)
    response_to_tabular(session, response, fetchsize; cached_schema=:functions)
end

"""
Execute a statement.
Statement to be executed can be DML, DDL, SET, etc.
Optional `config` parameter can have additional keyword parameters that will be passed as configuration 
    properties that are overlayed on top of the the existing session configuration before this statement
    is executed. They apply to this statement only and are not permanent.
When async is true, execution is asynchronous and a PendingResult may be returned.
If the returned value is a PendingResult:
    - Caller must call `isready` on a PendingResult instance to check for completion.
    - Once ready, calling `result` on it returns ResultSet (the same PendingResult instance is returned if it is still not ready)
"""
function execute(session::HiveSession, statement::AbstractString; async::Bool=false, config::Dict=Dict())
    conn = session.conn

    request = TExecuteStatementReq(; sessionHandle=conn.handle.sessionHandle, statement=statement, runAsync=async)
    if !isempty(config)
        cfg = Dict{String,String}()
        for (k,v) in config
            cfg[string(k)] = string(v)
        end
        request.confOverlay = cfg
    end

    response = ExecuteStatement(conn.client, request)
    response_to_resultset(session, response; force_pending=async)
end
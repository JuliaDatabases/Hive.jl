
function response_to_dataframe(session::HiveSession, response, fetchsize::Integer)
    ready = check_status(response.status)
    rs = result(session, ready, response.operationHandle)::ResultSet
    fetchsize!(rs, fetchsize)
    dataframe(rs)
end

##
# Returns the list of catalogs (databases).
# Results are ordered by TABLE_CATALOG.
function catalogs(session::HiveSession; fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn
    request = thriftbuild(TGetCatalogsReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    response = GetCatalogs(conn.client, request)
    response_to_dataframe(session, response, fetchsize)
end

##
# Retrieves the schema names available in this database.
# The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
function schemas(session::HiveSession; catalog_pattern::AbstractString="", schema_pattern::AbstractString="", fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = thriftbuild(TGetSchemasReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    isempty(catalog_pattern) || set_field!(request, :catalogName, catalog_pattern)
    isempty(schema_pattern) || set_field!(request, :schemaName, schema_pattern)

    response = GetSchemas(conn.client, request)
    response_to_dataframe(session, response, fetchsize)
end

##
# Returns a list of tables with catalog, schema, and table type information.
# Results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, and TABLE_NAME
function tables(session::HiveSession; catalog_pattern::AbstractString="", schema_pattern::AbstractString="", table_pattern::AbstractString="", table_types::Array=[], fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn

    request = thriftbuild(TGetTablesReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    isempty(catalog_pattern) || set_field!(request, :catalogName, catalog_pattern)
    isempty(schema_pattern) || set_field!(request, :schemaName, schema_pattern)
    isempty(table_pattern) || set_field!(request, :tableName, table_pattern)
    isempty(table_types) || set_field!(request, :tableTypes, convert(Array{UTF8String,1}, table_types))

    response = GetTables(conn.client, request)
    response_to_dataframe(session, response, fetchsize)
end

##
# Returns the table types available in this database.
# The results are ordered by table type.
function tabletypes(session::HiveSession; fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn
    request = thriftbuild(TGetTableTypesReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    response = GetTableTypes(conn.client, request)
    response_to_dataframe(session, response, fetchsize)
end

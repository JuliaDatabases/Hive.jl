##
# Catalogs
function catalogs(session::HiveSession, fetchsize::Integer=DEFAULT_FETCH_SIZE)
    conn = session.conn
    request = thriftbuild(TGetCatalogsReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    response = GetCatalogs(conn.client, request)
    ready = check_status(response.status)
    rs = result(session, ready, response.operationHandle)::ResultSet
    fetchsize!(rs, fetchsize)
    dataframe(rs)
end

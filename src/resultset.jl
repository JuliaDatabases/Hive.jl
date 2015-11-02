#
# Result
# can have either of the following:
# - recordset
# - number of rows affected
#
# pending results can be
# - checked for readiness
# - cancelled
#
# results must be closed

# number of records to fetch with every fetchnext
const DEFAULT_FETCH_SIZE = 1024

type PendingResult
    session::HiveSession
    handle::TOperationHandle
    status::Nullable{TGetOperationStatusReq}
end

typealias RowCount Float64

type ResultSet
    session::HiveSession
    handle::TOperationHandle
    schema::Nullable{TTableSchema}
    rowset::Nullable{TRowSet}
    position::Int
    fetchsize::Int
    eof::Bool

    function ResultSet(session::HiveSession, handle::TOperationHandle)
        new(session, handle, Nullable{TOperationHandle}(), Nullable{TRowSet}(), 0, DEFAULT_FETCH_SIZE, false)
    end
end

typealias Result Union{ResultSet, RowCount, PendingResult}

function result(session::HiveSession, ready::Bool, handle::TOperationHandle)
    ready || (return PendingResult(session, handle, Nullable{TGetOperationStatusReq}()))

    # hasResultSet == true => a result set (possibly empty) can be fetched, modifiedRowCount is not set.
    # hasResultSet == false => modifiedRowCount is set
    handle.hasResultSet && (return ResultSet(session, handle))

    # modifiedRowCount >= 0 => number of rows affected is known
    # modifiedRowCount < 0 => operation is capable fo modifying rows, but the count is unknown. e.g. LOAD DATA
    return response.modifiedRowCount
end

eof(r::ResultSet) = r.eof

close(pending::PendingResult) = close(pending.session, pending.handle)
close(resultset::ResultSet) = close(resultset.session, resultset.handle)
function close(session::HiveSession, handle::TOperationHandle)
    conn = session.conn
    request = thriftbuild(TCloseOperationReq, Dict(:operationHandle => handle))
    response = CloseOperation(conn.client, request)
    check_status(response.status)
end

cancel(pending::PendingResult) = cancel(pending.session, pending.handle)
cancel(resultset::ResultSet) = cancel(resultset.session, resultset.handle)
function cancel(session::HiveSession, handle::TOperationHandle)
    conn = session.conn
    request = thriftbuild(TCancelOperationReq, Dict(:operationHandle => handle))
    response = CancelOperation(conn.client, request)
    check_status(response.status)
end

function status(pending::PendingResult)
    if !isnull(pending.status)
        response = get(pending.status)
    else
        conn = pending.session.conn
        request = thriftbuild(TGetOperationStatusReq, Dict(:operationHandle => pending.handle))
        response = GetOperationStatus(conn.client, request)
        pending.status = Nullable(response)
        check_status(response.status)
    end
    response
end

const READY_STATUS = (TOperationState.FINISHED_STATE, TOperationState.CANCELED_STATE, TOperationState.CLOSED_STATE, TOperationState.ERROR_STATE)
const RESULT_STATUS = (TOperationState.FINISHED_STATE)
isready(::ResultSet) = true
isready(::RowCount) = true
isready(pending::PendingResult) = status(pending).operationState in READY_STATUS
hasresult(pending::PendingResult) = status(pending).operationState in RESULT_STATUS
function sqlerror(pending::PendingResult)
    isready(pending) || (return utf8(""))
    hasresult(pending) && (return utf8(""))
    response = get(pending.status)
    sqlstate = isfilled(response, :sqlState) ? getfield(response, :sqlState) : utf8("")
    errormessage = isfilled(response, :errorMessage) ? getfield(response, :errorMessage) : utf8("")
    errorcode = isfilled(response, :errorCode) ? getfield(response, :errorCode) : Int32(0)
    utf8("Error. $errormessage ($sqlstate, $errorcode)")
end

const SCHEMA_CACHE = Dict{Union{AbstractString,Symbol}, TTableSchema}()

function schema(rs::ResultSet; cached::Union{AbstractString,Symbol,Void}=nothing)
    if !isnull(rs.schema)
        sch = get(rs.schema)
    else
        if (cached !== nothing) && haskey(SCHEMA_CACHE, cached)
            sch = SCHEMA_CACHE[cached]
        else
            conn = rs.session.conn
            request = thriftbuild(TGetResultSetMetadataReq, Dict(:operationHandle => rs.handle))
            response = GetResultSetMetadata(conn.client, request)
            check_status(response.status)
            sch = response.schema
            (cached !== nothing) && (SCHEMA_CACHE[cached] = sch)
        end
        rs.schema = Nullable(sch)
    end
    sch
end

# Fetch rows from the server corresponding to a particular OperationHandle.
fetchsize(rs::ResultSet) = rs.fetchsize
fetchsize!(rs::ResultSet, sz::Integer) = (rs.fetchsize = sz)

function fetchfirst(rs::ResultSet, nrows::Integer=rs.fetchsize)
    rs.position = 0
    fetchnext(rs, nrows)
end

function fetchnext(rs::ResultSet, nrows::Integer=rs.fetchsize)
    rs.eof && return nothing
    conn = rs.session.conn
    orientation = rs.position > 0 ? TFetchOrientation.FETCH_NEXT : TFetchOrientation.FETCH_FIRST
    request = thriftbuild(TFetchResultsReq, Dict(
        :operationHandle => rs.handle,
        :orientation => orientation,
        :maxRows => nrows))
    response = FetchResults(conn.client, request)
    check_status(response.status)

    rs.eof = !response.hasMoreRows
    rowset = response.results
    rs.rowset = Nullable(rowset)
    rs.position = rowset.startRowOffset + length(rowset.rows)
    rs
end

function dataframe(rs::ResultSet)
    isnull(rs.rowset) && fetchnext(rs)

    rowset = get(rs.rowset)
    sch = schema(rs)
    ncols = length(sch.columns)
    colvecs = DataArray[]

    if isfilled(rowset, :columns)
        @logmsg("reading columns")
        # ref: https://issues.apache.org/jira/browse/HIVE-3746
        for col in rowset.columns
            push!(colvecs, julia_type(col))
        end
    else
        for T in coltypes(sch)
            push!(colvecs, DataArray(T[]))
        end
        @logmsg("reading rows for coltypes: $(coltypes(sch))")
        for row in rowset.rows
            colidx = 1
            for col in row.colVals
                push!(colvecs[colidx], julia_type(col))
                colidx += 1
            end
        end
    end

    df = DataFrame()
    for colidx in 1:ncols
        colname = sch.columns[colidx].columnName
        @logmsg("$colname: $(colvecs[colidx])")
        df[symbol(colname)] = colvecs[colidx]
    end
    df
end

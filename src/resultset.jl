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

"""number of records to fetch with every fetchnext"""
const DEFAULT_FETCH_SIZE = 1024 * 1024

mutable struct PendingResult
    session::HiveSession
    handle::TOperationHandle
    status::Union{Nothing,TGetOperationStatusResp}
end

const RowCount = Float64

mutable struct ResultSet
    session::HiveSession
    handle::TOperationHandle
    schema::Union{Nothing,TTableSchema}
    rowset::Union{Nothing,TRowSet}
    position::Int
    fetchsize::Int
    eof::Bool

    function ResultSet(session::HiveSession, handle::TOperationHandle)
        new(session, handle, nothing, nothing, 0, DEFAULT_FETCH_SIZE, false)
    end
end

function show(io::IO, rs::ResultSet)
    print(io, "ResultSet(eof=$(rs.eof), position=$(rs.position), fetchsize=$(rs.fetchsize))")
end

const Result = Union{ResultSet, RowCount, PendingResult}

result(pending::PendingResult) = isready(pending) ? result(pending.session, true, pending.handle) : pending
result(rs::ResultSet) = rs
function result(session::HiveSession, ready::Bool, handle::TOperationHandle)
    ready || (return PendingResult(session, handle, nothing))

    # hasResultSet == true => a result set (possibly empty) can be fetched, modifiedRowCount is not set.
    # hasResultSet == false => modifiedRowCount is set
    handle.hasResultSet && (return ResultSet(session, handle))

    # modifiedRowCount >= 0 => number of rows affected is known
    # modifiedRowCount < 0 => operation is capable fo modifying rows, but the count is unknown. e.g. LOAD DATA
    return hasproperty(handle, :modifiedRowCount) ? handle.modifiedRowCount : 0.0
end

eof(r::ResultSet) = r.eof

close(pending::PendingResult) = close(pending.session, pending.handle)
close(resultset::ResultSet) = close(resultset.session, resultset.handle)
function close(session::HiveSession, handle::TOperationHandle)
    conn = session.conn
    request = TCloseOperationReq(; operationHandle=handle)
    response = CloseOperation(conn.client, request)
    check_status(response.status)
end

cancel(pending::PendingResult) = cancel(pending.session, pending.handle)
cancel(resultset::ResultSet) = cancel(resultset.session, resultset.handle)
function cancel(session::HiveSession, handle::TOperationHandle)
    conn = session.conn
    request = TCancelOperationReq(; operationHandle=handle)
    response = CancelOperation(conn.client, request)
    check_status(response.status)
end

function status(pending::PendingResult)
    if pending.status === nothing
        conn = pending.session.conn
        request = TGetOperationStatusReq(; operationHandle=pending.handle)
        response = GetOperationStatus(conn.client, request)
        check_status(response.status)
        if response.operationState in READY_STATUS
            pending.status = response
        end
    end
    pending.status
end

const READY_STATUS = (TOperationState.FINISHED_STATE, TOperationState.CANCELED_STATE, TOperationState.CLOSED_STATE, TOperationState.ERROR_STATE)
const RESULT_STATUS = (TOperationState.FINISHED_STATE)
isready(::ResultSet) = true
isready(::RowCount) = true
isready(pending::PendingResult) = status(pending).operationState in READY_STATUS
hasresult(pending::PendingResult) = status(pending).operationState in RESULT_STATUS
function sqlerror(pending::PendingResult)
    isready(pending) || (return "")
    hasresult(pending) && (return "")
    response = pending.status
    sqlstate = hasproperty(response, :sqlState) ? response.sqlState : ""
    errormessage = hasproperty(response, :errorMessage) ? response.errorMessage : ""
    errorcode = hasproperty(response, :errorCode) ? response.errorCode : Int32(0)
    "Error. $errormessage ($sqlstate, $errorcode)"
end

const SCHEMA_CACHE = Dict{Union{AbstractString,Symbol}, TTableSchema}()

function schema(rs::ResultSet; cached::Union{AbstractString,Symbol,Nothing}=nothing)
    if rs.schema === nothing
        if (cached !== nothing) && haskey(SCHEMA_CACHE, cached)
            sch = SCHEMA_CACHE[cached]
        else
            conn = rs.session.conn
            request = TGetResultSetMetadataReq(; operationHandle=rs.handle)
            response = GetResultSetMetadata(conn.client, request)
            check_status(response.status)
            sch = response.schema
            (cached !== nothing) && (SCHEMA_CACHE[cached] = sch)
        end
        rs.schema = sch
    end
    rs.schema
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
    request = TFetchResultsReq(; operationHandle=rs.handle, orientation=orientation, maxRows=nrows)
    response = FetchResults(conn.client, request)
    check_status(response.status)

    rowset = response.results
    rs.rowset = rowset
    nfetched = 0
    if hasproperty(rowset, :columns)
        onecol = rowset.columns[1]
        for fld in propertynames(onecol)
            if hasproperty(onecol, fld)
                val = getproperty(onecol, fld)
                nfetched = length(val.values)
            end
        end
    else
        nfetched = length(rowset.rows)
    end

    if nfetched > 0
        rs.position += nfetched
    else
        rs.eof = !response.hasMoreRows
    end

    rs
end

##
# Iterators
# - records, record iterator: fetches one record at a time as a tuple
# - tabulars, tabular iterator: fetches one batch of records at a time, returns a Tabular
# - tabular(tabular iterator): fetches and returns all data as one Tabular
# - columnchunks, columnchunks iterator: fetched one batch of records at a time, returns a vector of pairs of column names and data
# - columnchunk(columnchunks iterator): fetched and returns all data as one column chunk

struct ColumnChunksIterator
    rs::ResultSet

    function ColumnChunksIterator(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE)
        fetchsize!(rs, fetchsz)
        new(rs)
    end
end


columnchunks(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE) = ColumnChunksIterator(rs, fetchsz)
columnchunk(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE) = columnchunk(ColumnChunksIterator(rs, fetchsz))
function columnchunk(iter::ColumnChunksIterator)
    iter_result = iterate(iter)
    if iter_result === nothing
        v0 = iter_result
    else
        v0, state = iter_result
        iter_result = iterate(iter, state)
        while iter_result !== nothing
            v1, state = iter_result
            v0 = [v0[idx][1]=>vcat(v0[idx][2], v1[idx][2]) for idx in 1:length(v0)]
            iter_result = iterate(iter, state)
        end
    end
    v0
end

iteratorsize(::Type{ColumnChunksIterator}) = Base.SizeUnknown()
function iterate(iter::ColumnChunksIterator, state_eof=iter.rs.eof)
    state_eof && (return nothing)

    rs = iter.rs
    fetchnext(rs)

    rowset = rs.rowset
    sch = schema(rs)
    ncols = length(sch.columns)
    cconvs = colconvfns(sch)

    if hasproperty(rowset, :columns)
        @debug("reading columns")
        # ref: https://issues.apache.org/jira/browse/HIVE-3746
        colvecs = [julia_type(col, colconv) for (col,colconv) in zip(rowset.columns,cconvs)]
    else
        ctypes = coltypes(sch)
        colvecs = [data_with_missings(T[]) for T in ctypes]
        @debug("reading rows for coltypes: $(coltypes(sch))")
        for row in rowset.rows
            colidx = 1
            for col in row.colVals
                push!(colvecs[colidx], julia_type(col, cconvs[colidx]))
                colidx += 1
            end
        end
    end

    cc = [Symbol(sch.columns[colidx].columnName)=>colvecs[colidx] for colidx in 1:ncols]
    cc, rs.eof
end

struct TabularIterator
    cc::ColumnChunksIterator
    dispargs::Any

    function TabularIterator(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE; kwargs...)
        new(ColumnChunksIterator(rs, fetchsz), kwargs)
    end
end

tabulars(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE; kwargs...) = TabularIterator(rs, fetchsz; kwargs...)
tabular(rs::ResultSet, fetchsz::Integer=DEFAULT_FETCH_SIZE; kwargs...) = tabular(tabulars(rs, fetchsz; kwargs...))
function tabular(iter::TabularIterator)
    t = reduce(vcat, iter)
    close(iter.cc.rs)
    t
end

iteratorsize(::Type{TabularIterator}) = Base.SizeUnknown()
function iterate(iter::TabularIterator, state_eof=iter.cc.rs.eof)
    state_eof && (return nothing)
    cols,eof = iterate(iter.cc, state_eof)
    Tabular(cols; iter.dispargs...), eof
end

mutable struct RecordIterator
    cciter::ColumnChunksIterator
    ccpos::Int
    cc::Union{Nothing,Vector{Pair{Symbol,Vector}}}

    RecordIterator(rs::ResultSet) = new(ColumnChunksIterator(rs), 1, nothing)
end

records(rs::ResultSet) = RecordIterator(rs)

iteratorsize(::Type{RecordIterator}) = Base.SizeUnknown()
function iterate(iter::RecordIterator, state_eof=iter.cciter.rs.eof)
    state_eof && (return nothing)
    (nothing !== iter.cc) && (iter.ccpos > length(iter.cc[1][2])) && (return nothing)

    if (nothing === iter.cc) || (iter.ccpos > length(iter.cc[1][2]))
        cc, state_eof = iterate(iter.cciter, state_eof)
        iter.cc = cc
        iter.ccpos = 1
    else
        cc = iter.cc
    end
    recs = (iter.ccpos > length(cc[1][2])) ? nothing : tuple([col[2][iter.ccpos] for col in cc]...)
    iter.ccpos += 1
    recs, state_eof
end

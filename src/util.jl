check_status(status::TStatus) = check_status(status.statusCode)
function check_status(status::Int32)
    if status == TStatusCode.SUCCESS_STATUS || status == TStatusCode.SUCCESS_WITH_INFO_STATUS
        return true
    end
    (status == TStatusCode.STILL_EXECUTING_STATUS) && (return false)
    (status == TStatusCode.ERROR_STATUS) && error("Hive operation failed")
    (status == TStatusCode.INVALID_HANDLE_STATUS) && error("Hive handle invalid")
    error("Unknown status code")
end


##
# Table schema methods
function dataframe(sch::TTableSchema)
    df = DataFrame()
    df[:position] = positions = DataArray(Int32[])
    df[:name] = names = DataArray(UTF8String[])
    df[:type] = types = DataArray(Type[])
    df[:comment] = comments = DataArray(UTF8String[])
    for col in sch.columns
        push!(comments, isfilled(col, :comment) ? getfield(col, :comment) : NA)
        push!(types, julia_type(col.typeDesc))
        push!(names, col.columnName)
        push!(positions, col.position)
    end
    df
end

show(io::IO, sch::TTableSchema) = show(io, dataframe(sch))

function coltypes(sch::TTableSchema)
    tuple([julia_type(col.typeDesc) for col in sch.columns]...)
end

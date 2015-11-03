function check_status(status::TStatus)
    errormsg = isfilled(status, :errorMessage) ? getfield(status, :errorMessage) : utf8("")
    infomsgs = isfilled(status, :infoMessages) ? getfield(status, :infoMessages) : UTF8String[]
    check_status(status.statusCode, errormsg, infomsgs)
end
function check_status(status::Int32, errormsg::AbstractString="", infomsgs::Array=[])
    if status == TStatusCode.SUCCESS_STATUS || status == TStatusCode.SUCCESS_WITH_INFO_STATUS
        map((msg)->println("$(msg)\n"), infomsgs)
        return true
    end
    (status == TStatusCode.STILL_EXECUTING_STATUS) && (return false)
    (status == TStatusCode.ERROR_STATUS) && error("Hive operation failed. $errormsg")
    (status == TStatusCode.INVALID_HANDLE_STATUS) && error("Hive handle invalid. $errormsg")
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

const BIT_MASKS = (0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80)
# the last byte of the bitset is always set to 0x00
function bitset_to_bools(data::Vector{UInt8}, L::Integer=((length(data)-1)*8))
    bools = Array(Bool, L)
    for idx in 1:L
        bools[idx] = (data[ceil(Int, idx/8)] & BIT_MASKS[rem(idx-1, 8) + 1] > 0x00)
    end
    bools
end

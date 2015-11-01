# Thrift types are supported
# All Hive types (complex types) are not supported yet

const JTYPES = Dict(
  Int32(0) => Bool,         # BOOLEAN
  Int32(1) => Int8,         # TINYINT
  Int32(2) => Int16,        # SMALLINT
  Int32(3) => Int32,        # INT
  Int32(4) => Int64,        # BIGINT
  Int32(5) => Float64,      # FLOAT
  Int32(6) => Float64,      # DOUBLE
  Int32(7) => UTF8String,   # STRING
  Int32(8) => Int64,        # TIMESTAMP
  Int32(9) => UTF8String,   # BINARY
  Int32(10) => UTF8String,  # ARRAY
  Int32(11) => UTF8String,  # MAP
  Int32(12) => UTF8String,  # STRUCT
  Int32(13) => UTF8String,  # UNIONTYPE
  Int32(15) => UTF8String,  # DECIMAL
  Int32(16) => UTF8String,  # NULL
  Int32(17) => UTF8String,  # DATE
  Int32(18) => UTF8String,  # VARCHAR
  Int32(19) => UTF8String,  # CHAR
  Int32(20) => UTF8String,  # INTERVAL_YEAR_MONTH
  Int32(21) => UTF8String   # INTERVAL_DAY_TIME
)

##
# map column types to Julia types
function julia_type(hs2type::TTypeDesc, typeentry::TPrimitiveTypeEntry)
    JTYPES[typeentry._type]
end

function julia_type(hs2type::TTypeDesc, typeentry::TArrayTypeEntry)
    typeptr = typeentry.objectTypePtr
    T = julia_type(hs2type, hs2type.types[typeptr])
    Array{T,1}
end

function julia_type(hs2type::TTypeDesc, typeentry::TMapTypeEntry)
    keytypeptr = typeentry.keyTypePtr
    valuetypeptr = typeentry.valueTypePtr
    K = julia_type(hs2type, hs2type.types[keytypeptr])
    V = julia_type(hs2type, hs2type.types[valuetypeptr])
    Dict{K,V}
end

function julia_type(hs2type::TTypeDesc, typeentry::TStructTypeEntry)
    Any
end

function julia_type(hs2type::TTypeDesc, typeentry::TUnionTypeEntry)
    Any
end

function julia_type(hs2type::TTypeDesc, typeentry::TUserDefinedTypeEntry)
    Any
end

function julia_type(hs2type::TTypeDesc, typeentry::TTypeEntry)
    for fld in fieldnames(TTypeEntry)
        isfilled(typeentry, fld) || continue
        return julia_type(hs2type, getfield(typeentry, fld))
    end
end

function julia_type(hs2type::TTypeDesc)
    # The "top" type is always the first element of the list.
    # If the top type is an ARRAY, MAP, STRUCT, or UNIONTYPE type, then subsequent elements represent nested types.
    toptype = hs2type.types[1]
    julia_type(hs2type, toptype)
end


##
# map column types to Julia type names
function julia_type_name(hs2type::TTypeDesc, typeentry::TPrimitiveTypeEntry)
    TYPE_NAMES[typeentry._type]
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TArrayTypeEntry)
    typeptr = typeentry.objectTypePtr
    typename = julia_type_name(hs2type, hs2type.types[typeptr])
    "Array{$typename}"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TMapTypeEntry)
    keytypeptr = typeentry.keyTypePtr
    valuetypeptr = typeentry.valueTypePtr
    keytypename = julia_type_name(hs2type, hs2type.types[keytypeptr])
    valuetypename = julia_type_name(hs2type, hs2type.types[valuetypeptr])
    "Dict{$keytypename, $valuetypename}"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TStructTypeEntry)
    "Struct"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TUnionTypeEntry)
    "Union"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TUserDefinedTypeEntry)
    "UserDefined"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TTypeEntry)
    for fld in fieldnames(TTypeEntry)
        isfilled(typeentry, fld) || continue
        return julia_type_name(hs2type, getfield(typeentry, fld))
    end
end

function julia_type_name(hs2type::TTypeDesc)
    # The "top" type is always the first element of the list.
    # If the top type is an ARRAY, MAP, STRUCT, or UNIONTYPE type, then subsequent elements represent nested types.
    toptype = hs2type.types[1]
    julia_type_name(hs2type, toptype)
end


##
# map column values to Julia types
# map columns into DataVectors
typealias ColValue Union{TBoolValue, TByteValue, TI16Value, TI32Value, TI64Value, TDoubleValue, TStringValue}
typealias Col  Union{TBoolColumn, TByteColumn, TI16Column, TI32Column, TI64Column, TDoubleColumn, TStringColumn, TBinaryColumn}

function julia_type{T<:ColValue}(colval::T)
    isfilled(colval, :value) ? getfield(colval, :value) : NA
end

function julia_type(colval::TColumnValue)
    for fld in fieldnames(TColumnValue)
        isfilled(colval, fld) || continue
        return julia_type(getfield(colval, fld))
    end
    NA
end

function julia_type{T<:Col}(col::T)
    if isempty(col.nulls) || isempty(col.values)
        DataArray(col.values)
    else
        DataArray(col.values, convert(Array{Bool}, col.nulls))
    end
end

function julia_type(col::TColumn)
    for fld in fieldnames(TColumn)
        isfilled(col, fld) || continue
        return julia_type(getfield(col, fld))
    end
    NA
end

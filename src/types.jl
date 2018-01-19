# Thrift types are supported
# All Hive types (complex types) are not supported yet

const JTYPES = Dict(
  Int32(0) => Bool,                 # BOOLEAN
  Int32(1) => Int8,                 # TINYINT
  Int32(2) => Int16,                # SMALLINT
  Int32(3) => Int32,                # INT
  Int32(4) => Int64,                # BIGINT
  Int32(5) => Float64,              # FLOAT
  Int32(6) => Float64,              # DOUBLE
  Int32(7) => String,               # STRING
  Int32(8) => Int64,                # TIMESTAMP
  Int32(9) => String,               # BINARY
  Int32(10) => String,              # ARRAY
  Int32(11) => String,              # MAP
  Int32(12) => String,              # STRUCT
  Int32(13) => String,              # UNIONTYPE
  Int32(15) => String,              # DECIMAL
  Int32(16) => String,              # NULL
  Int32(17) => String,              # DATE
  Int32(18) => String,              # VARCHAR
  Int32(19) => String,              # CHAR
  Int32(20) => String,              # INTERVAL_YEAR_MONTH
  Int32(21) => String               # INTERVAL_DAY_TIME
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
const ColValue = Union{TBoolValue, TByteValue, TI16Value, TI32Value, TI64Value, TDoubleValue, TStringValue}
const Col = Union{TBoolColumn, TByteColumn, TI16Column, TI32Column, TI64Column, TDoubleColumn, TStringColumn, TBinaryColumn}

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
    if (length(col.nulls) < 2) || isempty(col.values)
        DataArray(col.values)
    else
        values = col.values
        nulls = bitset_to_bools(col.nulls, length(values))
        DataArray(values, nulls)
    end
end

function julia_type(col::TColumn)
    for fld in fieldnames(TColumn)
        isfilled(col, fld) || continue
        return julia_type(getfield(col, fld))
    end
    NA
end

# Thrift types are supported
# All Hive types (complex types) are not supported yet

const JTYPES = Dict(
  Int32(0)  => Bool,                # BOOLEAN
  Int32(1)  => Int8,                # TINYINT
  Int32(2)  => Int16,               # SMALLINT
  Int32(3)  => Int32,               # INT
  Int32(4)  => Int64,               # BIGINT
  Int32(5)  => Float32,             # FLOAT
  Int32(6)  => Float64,             # DOUBLE
  Int32(7)  => String,              # STRING
  Int32(8)  => DateTime,            # TIMESTAMP
  Int32(9)  => String,              # BINARY
  Int32(10) => String,              # ARRAY
  Int32(11) => String,              # MAP
  Int32(12) => String,              # STRUCT
  Int32(13) => String,              # UNIONTYPE
  Int32(15) => BigFloat,            # DECIMAL
  Int32(16) => Void,                # NULL
  Int32(17) => Date,                # DATE
  Int32(18) => String,              # VARCHAR
  Int32(19) => String,              # CHAR                  (TODO: can be optimized for CHAR(1))
  Int32(20) => String,              # INTERVAL_YEAR_MONTH
  Int32(21) => String               # INTERVAL_DAY_TIME
)

function with_null_check(fn, str::String)
    str = strip(str)
    isempty(str) ? NA : fn(str)
end

tochar(str::String) = isempty(str) ? Char(0) : first(str)
tobigfloat(str::String) = with_null_check(BigFloat, str)
todate(str::String) = with_null_check(Date, str)
toint8(val::UInt8) = reinterpret(Int8, val)
todatetime(ftime) = Dates.unix2datetime(ftime)
function todatetime(str::String)
    with_null_check(str) do str
        contains(str, "-") ? DateTime(replace(str, " ", "T")) : Dates.unix2datetime(parse(Int, str))
    end
end


const JCONV = Dict(
  Int32(0)  => nothing,             # BOOLEAN
  Int32(1)  => toint8,              # TINYINT
  Int32(2)  => nothing,             # SMALLINT
  Int32(3)  => nothing,             # INT
  Int32(4)  => nothing,             # BIGINT
  Int32(5)  => Float32,             # FLOAT
  Int32(6)  => nothing,             # DOUBLE
  Int32(7)  => nothing,             # STRING
  Int32(8)  => todatetime,          # TIMESTAMP
  Int32(9)  => nothing,             # BINARY
  Int32(10) => nothing,             # ARRAY
  Int32(11) => nothing,             # MAP
  Int32(12) => nothing,             # STRUCT
  Int32(13) => nothing,             # UNIONTYPE
  Int32(15) => tobigfloat,          # DECIMAL
  Int32(16) => nothing,             # NULL
  Int32(17) => todate,              # DATE
  Int32(18) => nothing,             # VARCHAR
  Int32(19) => nothing,             # CHAR
  Int32(20) => nothing,             # INTERVAL_YEAR_MONTH
  Int32(21) => nothing              # INTERVAL_DAY_TIME
)

##
# map column types to Julia types
function julia_type(hs2type::TTypeDesc, typeentry::TPrimitiveTypeEntry)
    if (typeentry._type == 19) && isfilled(typeentry, :typeQualifiers) && isfilled(typeentry.typeQualifiers, :qualifiers)
        ("characterMaximumLength" in keys(typeentry.typeQualifiers.qualifiers)) && (typeentry.typeQualifiers.qualifiers["characterMaximumLength"].i32Value == 1) && (return Char)
    end
    JTYPES[typeentry._type]
end

function julia_conv(hs2type::TTypeDesc, typeentry::TPrimitiveTypeEntry)
    if (typeentry._type == 19) && isfilled(typeentry, :typeQualifiers) && isfilled(typeentry.typeQualifiers, :qualifiers)
        ("characterMaximumLength" in keys(typeentry.typeQualifiers.qualifiers)) && (typeentry.typeQualifiers.qualifiers["characterMaximumLength"].i32Value == 1) && (return tochar)
    end
    JCONV[typeentry._type]
end

function julia_type(hs2type::TTypeDesc, typeentry::TArrayTypeEntry)
    typeptr = typeentry.objectTypePtr
    T = julia_type(hs2type, hs2type.types[typeptr])
    Vector{T}
end

function julia_type(hs2type::TTypeDesc, typeentry::TMapTypeEntry)
    keytypeptr = typeentry.keyTypePtr
    valuetypeptr = typeentry.valueTypePtr
    K = julia_type(hs2type, hs2type.types[keytypeptr])
    V = julia_type(hs2type, hs2type.types[valuetypeptr])
    Dict{K,V}
end

julia_type(hs2type::TTypeDesc, typeentry::TStructTypeEntry) = Any
julia_type(hs2type::TTypeDesc, typeentry::TUnionTypeEntry) = Any
julia_type(hs2type::TTypeDesc, typeentry::TUserDefinedTypeEntry) = Any

function julia_type(hs2type::TTypeDesc, typeentry::TTypeEntry)
    for fld in fieldnames(TTypeEntry)
        isfilled(typeentry, fld) && (return julia_type(hs2type, getfield(typeentry, fld)))
    end
end

function julia_conv(hs2type::TTypeDesc, typeentry::TTypeEntry)
    for fld in fieldnames(TTypeEntry)
        isfilled(typeentry, fld) && (return julia_conv(hs2type, getfield(typeentry, fld)))
    end
end

function julia_type(hs2type::TTypeDesc)
    # The "top" type is always the first element of the list.
    # If the top type is an ARRAY, MAP, STRUCT, or UNIONTYPE type, then subsequent elements represent nested types.
    toptype = hs2type.types[1]
    julia_type(hs2type, toptype)
end

function julia_conv(hs2type::TTypeDesc)
    # The "top" type is always the first element of the list.
    # If the top type is an ARRAY, MAP, STRUCT, or UNIONTYPE type, then subsequent elements represent nested types.
    toptype = hs2type.types[1]
    julia_conv(hs2type, toptype)
end


##
# map column types to Julia type names
julia_type_name(hs2type::TTypeDesc, typeentry::TPrimitiveTypeEntry) = TYPE_NAMES[typeentry._type]

function julia_type_name(hs2type::TTypeDesc, typeentry::TArrayTypeEntry)
    typeptr = typeentry.objectTypePtr
    typename = julia_type_name(hs2type, hs2type.types[typeptr])
    "Vector{$typename}"
end

function julia_type_name(hs2type::TTypeDesc, typeentry::TMapTypeEntry)
    keytypeptr = typeentry.keyTypePtr
    valuetypeptr = typeentry.valueTypePtr
    keytypename = julia_type_name(hs2type, hs2type.types[keytypeptr])
    valuetypename = julia_type_name(hs2type, hs2type.types[valuetypeptr])
    "Dict{$keytypename, $valuetypename}"
end

julia_type_name(hs2type::TTypeDesc, typeentry::TStructTypeEntry) = "Struct"
julia_type_name(hs2type::TTypeDesc, typeentry::TUnionTypeEntry) = "Union"
julia_type_name(hs2type::TTypeDesc, typeentry::TUserDefinedTypeEntry) = "UserDefined"

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

julia_type(colval::T, convfn::Void) where T<:ColValue = isfilled(colval, :value) ? getfield(colval, :value) : NA
julia_type(colval::T, convfn) where T<:ColValue = isfilled(colval, :value) ? convfn(getfield(colval, :value)) : NA

function julia_type(colval::TColumnValue, convfn)
    for fld in fieldnames(TColumnValue)
        isfilled(colval, fld) && (return julia_type(getfield(colval, fld), convfn))
    end
    NA
end

function julia_type(col::T, convfn) where T<:Col
    if (length(col.nulls) < 2) || isempty(col.values)
        map(convfn, col.values)
    else
        nulls = bitset_to_bools(col.nulls, length(col.values))
        DataArray(map(convfn, col.values), nulls)
    end
end

function julia_type(col::T, convfn::Void) where T<:Col
    if (length(col.nulls) < 2) || isempty(col.values)
        col.values
    else
        nulls = bitset_to_bools(col.nulls, length(col.values))
        DataArray(col.values, nulls)
    end
end

function julia_type(col::TColumn, convfn)
    for fld in fieldnames(TColumn)
        isfilled(col, fld) && (return julia_type(getfield(col, fld), convfn))
    end
    NA
end

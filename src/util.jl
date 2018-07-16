function check_status(status::TStatus)
    errormsg = isfilled(status, :errorMessage) ? getfield(status, :errorMessage) : String("")
    infomsgs = isfilled(status, :infoMessages) ? getfield(status, :infoMessages) : String[]
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

const BIT_MASKS = (0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80)
# the last byte of the bitset is always set to 0x00
function bitset_to_bools(data::Vector{UInt8}, L::Integer=((length(data)-1)*8))
    bools = Array{Bool}(L)
    LD = length(data)
    for idx in 1:L
        didx = ceil(Int, idx/8)
        bools[idx] = (didx > LD) ? false : (data[didx] & BIT_MASKS[rem(idx-1, 8) + 1] > 0x00)
    end
    bools
end

function data_with_missings{T}(data::Vector{T}, mpos::Vector{Bool}=Bool[])
    LD = length(data)
    LM = length(mpos)
    result = Vector{Union{T,Nothing}}(undef, LD)
    for idx in 1:LD
        result[idx] = ((idx <= LM) && mpos[idx]) ? nothing : data[idx]
    end
    result
end

##
# show methods
function show_table(io::IO, t; header=nothing, cnames=[n for (n,v) in t], divider=nothing, cstyle=[], full=false, ellipsis=:middle, compact_if_too_wide=true)
    height, width = displaysize(io)
    showrows = height-5 - (header !== nothing)
    n = N = length(t[1][2])
    nc = length(t)
    header !== nothing && println(io, header)
    if full
        rows = [1:n;]
        showrows = n
    else
        if ellipsis == :middle
            lastfew = div(showrows, 2) - 1
            firstfew = showrows - lastfew - 1
            rows = n > showrows ? [1:firstfew; (n-lastfew+1):n] : [1:n;]
        elseif ellipsis == :end
            lst = n == showrows ?
                showrows : showrows-1 # make space for ellipse
            rows = [1:min(N, showrows);]
        else
            error("ellipsis must be either :middle or :end")
        end
    end
    cols = [v for (n,v) in t]
    reprs  = [ sprint(io->showcompact(io,cols[j][i])) for i in rows, j in 1:nc ]
    strcnames = map(string, cnames)
    widths  = [ max(strwidth(get(strcnames, c, "")), isempty(reprs) ? 0 : maximum(map(strwidth, reprs[:,c]))) for c in 1:nc ]
    if compact_if_too_wide && ((sum(widths) + 2*nc) > width)
        return show_table_meta(io, t, cnames)
    end
    for c in 1:nc
        nm = get(strcnames, c, "")
        style = get(cstyle, c, nothing)
        txt = c==nc ? nm : rpad(nm, widths[c]+(c==divider ? 1 : 2), " ")
        if style == nothing
            print(io, txt)
        else
            with_output_format(style, print, io, txt)
        end
        if c == divider
            print(io, "│")
            length(cnames) > divider && print(io, " ")
        end
    end
    println(io)
    if divider !== nothing
        print(io, "─"^(sum(widths[1:divider])+2*divider-1), "┼", "─"^(sum(widths[divider+1:end])+2*(nc-divider)-1))
    else
        print(io, "─"^(sum(widths)+2*nc-2))
    end
    for r in 1:size(reprs,1)
        println(io)
        for c in 1:nc
            print(io, c==nc ? reprs[r,c] : rpad(reprs[r,c], widths[c]+(c==divider ? 1 : 2), " "))
            if c == divider
                print(io, "│ ")
            end
        end
        if n > showrows && ((ellipsis == :middle && r == firstfew) || (ellipsis == :end && r == size(reprs, 1)))
            if divider === nothing
                println(io)
                print(io, "⋮")
            else
                println(io)
                print(io, " "^(sum(widths[1:divider]) + 2*divider-1), "⋮")
            end
        end
    end
end

function show_table_meta(io, t, cnames)
    nc = length(t)
    println(io, "Columns:")
    cols = [v for (n,v) in t]
    coltypes = map(x->string(eltype(x)), cols)
    metat = (Symbol("#")=>[1:nc;], :colname=>cnames, Symbol("type")=>coltypes)
    show_table(io, metat; cstyle=fill(:bold, nc), full=true, compact_if_too_wide=false)
end

struct Tabular
    data::Vector{Pair}
    dispargs::Vector{Any}

    Tabular(data; kwargs...) = new(data, kwargs)
end


function vcat(t1::Tabular, t2::Tabular)
    data = [x[1][1]=>vcat(x[1][2], x[2][2]) for x in zip(t1.data, t2.data)]
    dispargset = Dict()
    for t in (t1,t2)
        for (n,v) in t.dispargs
            dispargset[n] = v
        end
    end
    dispargs = Any[(n,v) for (n,v) in dispargset]
    Tabular(data; dispargs...)
end

##
# Table schema methods
function tabular(sch::TTableSchema)
    positions = Vector{Int32}()
    names = Vector{String}()
    types = Vector{Type}()
    comments = Vector{Union{String,Nothing}}()
    for col in sch.columns
        push!(comments, isfilled(col, :comment) ? getfield(col, :comment) : nothing)
        push!(types, julia_type(col.typeDesc))
        push!(names, col.columnName)
        push!(positions, col.position)
    end
    Tabular([:position=>positions, :name=>names, :type=>types, :comment=>comments])
end

show(io::IO, sch::TTableSchema) = show(io, tabular(sch))
show(io::IO, t::Tabular) = show_table(io, t.data; t.dispargs...)

coltypes(sch::TTableSchema) = [julia_type(col.typeDesc) for col in sch.columns]
colconvfns(sch::TTableSchema) = [julia_conv(col.typeDesc) for col in sch.columns]

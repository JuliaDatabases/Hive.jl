##
# Authentication mechanisms
# Only SASL-Plain supported for now
type HiveAuth
    mechanism::AbstractString
    callback::Function

    HiveAuth(mechanism::AbstractString=SASL_MECH_PLAIN, callback::Function=Thrift.sasl_callback_default) = new(mechanism, callback)
end

function HiveAuthSASLPlain(uid::AbstractString, passwd::AbstractString; zid::AbstractString="")
    function callback(part::Symbol)
        (part == :authcid) && (return uid)
        (part == :passwd) && (return passwd)
        (part == :show) && (return uid)
        (part == :mechanism) && (return "SASL-Plain")
        return zid
    end
    HiveAuth(SASL_MECH_PLAIN, callback)
end

function show(io::IO, auth::HiveAuth)
    uid = auth.callback(:show)
    mech = auth.callback(:mechanism)
    println(io, "HiveAuth ($mech): $uid")
    nothing
end


##
# HiveConn holds the thrift connection and protocol objects.
# It also holds the hive session handle for this connection.
type HiveConn
    transport::TTransport
    protocol::TProtocol
    client::TCLIServiceClient
    handle::TOpenSessionResp
    connstr::AbstractString

    function HiveConn(host::AbstractString, port::Integer, auth::HiveAuth)
        transport = TSASLClientTransport(TSocket(host, port), auth.mechanism, auth.callback)
        protocol = TBinaryProtocol(transport, true)
        client = TCLIServiceClient(protocol)
        uid = auth.callback(:show)
        connstr = "hive2://$(uid)@$(host):$port"
        new(transport, protocol, client, connect(transport, client), connstr)
    end

    function connect(transport::TTransport, client::TCLIServiceClient)
        open(transport)
        request = thriftbuild(TOpenSessionReq, Dict(:client_protocol => TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8))
        response = OpenSession(client, request)
        check_status(response.status)
        response
    end
end

function show(io::IO, conn::HiveConn)
    println(io, conn.connstr)
    nothing
end

function close(conn::HiveConn)
    request = thriftbuild(TCloseSessionReq, Dict(:sessionHandle => conn.handle.sessionHandle))
    response = CloseSession(conn.client, request)
    check_status(response.status)
end

#
# HiveSession holds a connection and session status
type HiveSession
    conn::HiveConn

    function HiveSession(host::AbstractString="localhost", port::Integer=10000, auth::HiveAuth=HiveAuth())
        new(HiveConn(host, port, auth))
    end
end

function show(io::IO, sess::HiveSession)
    print(io, "HiveSession: ")
    show(io, sess.conn)
end
close(session::HiveSession) = close(session.conn)

##
# Info Type
const InfoType = TGetInfoType
function get_info(session::HiveSession, info_type::Int32)
    conn = session.conn
    request = thriftbuild(TGetInfoReq, Dict(:sessionHandle => conn.handle.sessionHandle, :infoType => info_type))
    response = GetInfo(conn.client, request)
    check_status(response.status)

    val = response.infoValue
    for fldname in fieldnames(TGetInfoValue)
        isfilled(val, fldname) && (return getfield(val, fldname))
    end
    nothing
end

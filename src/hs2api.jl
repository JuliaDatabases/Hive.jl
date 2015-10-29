using .HS2

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
        return zid
    end
    HiveAuth(SASL_MECH_PLAIN, callback)
end


##
# HiveConn holds the thrift connection and protocol objects.
# It also holds the hive session handle for this connection.
type HiveConn
    transport::TTransport
    protocol::TProtocol
    client::TCLIServiceClient
    handle::TOpenSessionResp

    function HiveConn(host::AbstractString, port::Integer, auth::HiveAuth)
        transport = TSASLClientTransport(TSocket(host, port), auth.mechanism, auth.callback)
        protocol = TBinaryProtocol(transport, true)
        client = TCLIServiceClient(protocol)
        new(transport, protocol, client, connect(transport, client))
    end

    function connect(transport::TTransport, client::TCLIServiceClient)
        open(transport)
        request = thriftbuild(TOpenSessionReq, Dict(:client_protocol => TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8))
        response = OpenSession(client, request)
        check_status(response.status)
        response
    end
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

close(session::HiveSession) = close(session.conn)

check_status(status::TStatus) = check_status(status.statusCode)
function check_status(status::Int32)
    if status == TStatusCode.SUCCESS_STATUS || status == TStatusCode.SUCCESS_WITH_INFO_STATUS
        return 0
    end
    (status == TStatusCode.STILL_EXECUTING_STATUS) && (return 1)
    (status == TStatusCode.ERROR_STATUS) && error("Hive operation failed")
    (status == TStatusCode.INVALID_HANDLE_STATUS) && error("Hive handle invalid")
    error("Unknown status code")
end

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

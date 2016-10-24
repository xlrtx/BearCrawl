import xmlrpclib
from error import RPCDBError, RPCQueueFull
from SimpleXMLRPCServer import SimpleXMLRPCServer


class Code:
    def __init__(self):
        pass

    QUEUE_FULL = -1
    DB_ERR = -2
    UNKNOWN_ERR = -3
    SUCCESS = 0


class RPCServer:
    def __init__(self, dbm, tqs, rpc_port=60000):
        self.__rpc_server = SimpleXMLRPCServer(('localhost', rpc_port,), allow_none=True, logRequests=False)
        self.__dbm = dbm
        self.__tqs = tqs
        print '[RPC Server] At 127.0.0.1:', rpc_port

    def register_function(self, func, name):
        self.__rpc_server.register_function(func, name)

    def serve_forever(self):
        print '[RPC Server] Serving'
        self.__rpc_server.serve_forever()

    @classmethod
    def shutdown(cls):
        print '[RPC Server] Shutting down'
        # TODO Find a better way


class RPCProxy:
    def __init__(self, rpc_url=None):
        rpc_url = rpc_url or 'http://localhost:60000'
        self.__rpc_proxy = xmlrpclib.ServerProxy(rpc_url, allow_none=True)

    def __getattr__(self, item):
        func = getattr(self.__rpc_proxy, item)

        def __func(*args, **kwargs):
            ret = func(args, kwargs)
            if ret['code'] == Code.QUEUE_FULL:
                raise RPCQueueFull
            elif ret['code'] == Code.DB_ERR:
                raise RPCDBError
            elif ret['code'] == Code.SUCCESS:
                return ret['data']

        return __func

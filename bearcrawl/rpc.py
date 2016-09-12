import xmlrpclib
from error import RPCDBError, RPCQueueFull
from SimpleXMLRPCServer import SimpleXMLRPCServer
from pymongo.errors import PyMongoError
from Queue import Full
from utils import gen_uuid


class Code:
    def __init__(self):
        pass

    RPC_CODE_QUEUE_FULL = -1
    RPC_CODE_DB_ERR = -2
    RPC_CODE_SUCCESS = 0


def rpc_server(func):
    def inner(*args, **kwargs):
        ret = {
            'code': Code.RPC_CODE_SUCCESS,
            'data': ''
        }
        try:
            ret['data'] = func(args[0], *args[1], **args[2])
        except PyMongoError:
            ret['code'] = Code.RPC_CODE_DB_ERR
        except Full:
            ret['code'] = Code.RPC_CODE_QUEUE_FULL
        except Exception:
            raise
        return ret
    return inner


class RPCServer:
    def __init__(self, dbm, tqs, rpc_port=60000):
        self.__rpc_server = SimpleXMLRPCServer(('localhost', rpc_port,), allow_none=True, logRequests=False)
        self.__dbm = dbm
        self.__tqs = tqs

        self.register_function(self.list_unfinished_task_basics, 'list_unfinished_task_basics')
        self.register_function(self.list_task_status, 'list_task_status')
        self.register_function(self.put_task, 'put_task')
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

    @rpc_server
    def list_unfinished_task_basics(self):
        return self.__dbm.list_unfinished_task_basics()

    @rpc_server
    def list_task_status(self):
        return self.__dbm.list_task_status()

    @rpc_server
    def put_task(self, domain, module, params, uuid=None):
        self.__tqs.put_task(domain, module, params, uuid=uuid)
        task = {
            'uuid': uuid or gen_uuid(),
            'domain': domain,
            'module': module,
            'params': params
        }
        self.__dbm.update_task_basics(task)
        self.__dbm.update_task_status_queueing(task)
        return None


class RPCProxy:
    def __init__(self, rpc_url=None):
        rpc_url = rpc_url or 'http://localhost:60000'
        self.__rpc_proxy = xmlrpclib.ServerProxy(rpc_url, allow_none=True)

    def __getattr__(self, item):
        func = getattr(self.__rpc_proxy, item)

        def __func(*args, **kwargs):
            ret = func(args, kwargs)
            if ret['code'] == Code.RPC_CODE_QUEUE_FULL:
                raise RPCQueueFull
            elif ret['code'] == Code.RPC_CODE_DB_ERR:
                raise RPCDBError
            elif ret['code'] == Code.RPC_CODE_SUCCESS:
                return ret['data']

        return __func

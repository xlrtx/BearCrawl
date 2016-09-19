from rpc import Code as RPCCode
from rpc import RPCServer
from utils import gen_uuid
from db import DatabaseManager
from task_queue import TaskQueueServer
import sys
import signal
from time import sleep
from Queue import Full
from threading import Thread
from pymongo.errors import PyMongoError


def rpc(func):
    def inner(*args, **kwargs):
        ret = {
            'code': RPCCode.SUCCESS,
            'data': ''
        }
        try:
            ret['data'] = func(args[0], *args[1], **args[2])
        except PyMongoError:
            ret['code'] = RPCCode.DB_ERR
        except Full:
            ret['code'] = RPCCode.QUEUE_FULL
        except Exception:
            raise
        return ret

    return inner


class BaseMaster(object):
    def __init__(self, queue_password, queue_port=50000, queue_size=0, rpc_port=60000, mongo_conn=None,
                 redis_client=None):
        # Setup task queue, workers use task queue
        self.__tqs = TaskQueueServer(queue_password, queue_port, queue_size)

        # Setup mongo and redis and load unfinished tasks
        self.__dbm = DatabaseManager(mongo_conn=mongo_conn, redis_client=redis_client)
        self.load_unfinished_tasks()

        # Setup rpc server, register master functions for cli, web to use.
        self.__rpcs = RPCServer(self.__dbm, self.__tqs, rpc_port=rpc_port)
        self.__rpcs.register_function(self.list_unfinished_task_basics, 'list_unfinished_task_basics')
        self.__rpcs.register_function(self.list_task_status, 'list_task_status')
        self.__rpcs.register_function(self.put_task, 'put_task')

        # Create thread and serve rpc forever
        thread_rpcs = RPCServerThread(self.__rpcs)
        thread_rpcs.setDaemon(True)
        thread_rpcs.start()

        # Setup signal
        signal.signal(signal.SIGINT, self.signal_handler)

        # Works forever
        while True:
            sleep(1)

        thread_rpcs.join()

    def signal_handler(self, signum, frame):
        self.__tqs.shutdown()
        self.__rpcs.shutdown()
        sys.exit(0)

    def load_unfinished_tasks(self):
        tasks = self.__dbm.list_unfinished_task_basics()
        for task in tasks:
            try:
                self.__tqs.put_task(**task)
            except Full:
                print 'Task queue is full.'
                continue
            else:
                self.__dbm.update_task_basics(task)
                self.__dbm.update_task_status_queueing(task)
        print '[Task Queue Server] Load', len(tasks), 'unfinished tasks'

    def print_unfinished_tasks(self):
        self.__dbm.list_unfinished_task_basics(verbal=True)

    @rpc
    def list_unfinished_task_basics(self):
        return self.__dbm.list_unfinished_task_basics()

    @rpc
    def list_task_status(self):
        return self.__dbm.list_task_status()

    @rpc
    def put_task(self, domain, module, params, uuid=None):
        task = {
            'uuid': uuid or gen_uuid(),
            'domain': domain,
            'module': module,
            'params': params
        }
        self.__tqs.put_task(domain, module, params, uuid)
        self.__dbm.update_task_basics(task)
        self.__dbm.update_task_status_queueing(task)
        return None


class RPCServerThread(Thread):
    def __init__(self, rpc_server):
        super(RPCServerThread, self).__init__()
        self.__rpcs = rpc_server

    def run(self):
        self.__rpcs.serve_forever()

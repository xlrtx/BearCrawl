from rpc import RPCServer
from db import DatabaseManager
from task_queue import TaskQueueServer
import sys
import signal
from time import sleep
from threading import Thread


class BaseMaster(object):
    def __init__(self, queue_password, queue_port=50000, queue_size=10, rpc_port=60000, mongo_conn=None,
                 redis_client=None):
        # Setup task queue, workers use task queue
        self.__tqs = TaskQueueServer(queue_password, queue_port, queue_size)

        # Setup mongo and redis and load unfinished tasks
        self.__dbm = DatabaseManager(mongo_conn=mongo_conn, redis_client=redis_client)
        self.__tqs.load_unfinished_tasks(self.__dbm)

        # Setup rpc server, register master functions for cli, web to use.
        self.__rpcs = RPCServer(self.__dbm, self.__tqs, rpc_port=rpc_port)

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


class RPCServerThread(Thread):
    def __init__(self, rpc_server):
        super(RPCServerThread, self).__init__()
        self.__rpcs = rpc_server

    def run(self):
        self.__rpcs.serve_forever()

# TODO RPC code, msg, data, rpc kwargs

import abc
import sys
import traceback
from db import DatabaseManager
from task_queue import TaskQueueClient
from error import TaskHandlerNotImplemented


class BaseWorker(object):
    def __init__(self, queue_password, queue_addr='127.0.0.1', queue_port=50000, mongo_conn=None,
                 redis_client=None):

        # Setup mongo and redis
        self.__dbm = DatabaseManager(mongo_conn=mongo_conn, redis_client=redis_client)

        # Setup task queue
        self.__tqc = TaskQueueClient(queue_password, queue_addr=queue_addr, queue_port=queue_port)

        # Current task
        self.__current_task = None

    def __consume_queue(self):
        task = self.__tqc.get_task()
        self.__current_task = task
        self.__dbm.update_task_status_working(task)
        try:
            self.task_handler(task['uuid'], task['domain'], task['module'], task['params'])
        except Exception, e:
            # TODO add error to detail
            self.__dbm.update_task_status_error(task, detail=e.message)
            exec_info = sys.exc_info()
            traceback.print_exception(*exec_info)
            return False
        else:
            self.__dbm.update_task_status_finish(task)
            return True

    def start(self):
        while True:
            self.__consume_queue()

    @abc.abstractmethod
    def task_handler(self, uuid, domain, module, params):
        raise TaskHandlerNotImplemented('Task handler not implemented')

    def save_dump(self, data):
        """
        Save data into mongo domain.module, need _id field to enable duplicate detection.
        :param data:
        :return: duplicate count
        """
        return self.__dbm.save_dump(self.__current_task, data)

    def update_progress(self, dict_progress):
        """
        Update dict_progress to mongo task.task.progress
        :param dict_progress:
        :return:
        """
        self.__dbm.update_task_progress(self.__current_task, dict_progress)

    def read_progress(self, key):
        """
        Read dict_progress from mongo task.task.progress
        :param key: progress key
        :return: progress key's value
        """
        progress = self.__dbm.read_task_progress(self.__current_task)
        if not progress:
            return None
        else:
            return progress.get(key, None)

    def update_filter(self, filter_name, items):
        self.__dbm.update_filter(self.__current_task, filter_name, items)

    def filter(self, filter_name, items):
        return self.__dbm.filter(self.__current_task, filter_name, items)

import sys
import traceback
from db import DatabaseManager
from error import TaskHandlerNotImplemented
from multiprocessing.managers import BaseManager as QueueManager
import abc


class BaseWorker(object):
    def __init__(self, queue_password, queue_addr='127.0.0.1', queue_port=50000, mongo_conn=None,
                 redis_client=None):

        # Register task queue
        QueueManager.register('get_task_queue')

        # Bind localhost
        manager = QueueManager(address=(queue_addr, queue_port), authkey=queue_password)
        manager.connect()

        # Get queue from manager
        self.__task_queue = manager.get_task_queue()

        # Setup mongo and redis
        self.__dbm = DatabaseManager(mongo_conn=mongo_conn, redis_client=redis_client)

        # Current task
        self.__current_task = None

    def __get_task(self):
        task = self.__task_queue.get()
        self.__task_queue.task_done()
        return task

    def __consume_queue(self):
        task = self.__get_task()
        self.__current_task = task
        self.__dbm.update_task_status_working(task)
        try:
            self.task_handler(task)
        except Exception:
            # TODO add error to detail
            self.__dbm.update_task_status_error(task)
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
    def task_handler(self, task):
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

    def read_progress(self):
        """
        Read dict_progress from mongo task.task.progress
        :return: dict_progress
        """
        return self.__dbm.read_task_progress(self.__current_task)

    def update_filter(self, filter_name, items):
        self.__dbm.update_filter(self.__current_task, filter_name, items)

    def filter(self, filter_name, items):
        return self.__dbm.filter(self.__current_task, filter_name, items)

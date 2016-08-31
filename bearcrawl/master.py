from Queue import Queue
from Queue import Full
import json
from time import sleep
from utils import gen_uuid
from db import DatabaseManager
from multiprocessing.managers import BaseManager as QueueManager


class BaseMaster(object):
    def __init__(self, queue_password, queue_port=50000, queue_size=10, mongo_conn=None, redis_client=None):

        # Setup task queue
        task_queue = Queue(maxsize=queue_size)

        # Register task queue
        QueueManager.register('get_task_queue', callable=lambda: task_queue)

        # Bind localhost
        manager = QueueManager(address=('', queue_port), authkey=queue_password)
        manager.start()

        # Get queue from manager
        self.__task_queue = manager.get_task_queue()

        # Setup mongo and redis
        self.__dbm = DatabaseManager(mongo_conn=mongo_conn, redis_client=redis_client)

        # Load unfinished tasks to queue
        tasks = self.list_task_unfinished()
        for task in tasks:
            self.put_task(**task)

    def put_task(self, domain, module, params, uuid=None):
        """
        Put a task into queue
        :param domain: Domain name indicating which db name mongo will use and prefix redis key will use,
            you may want to name base on which site its crawling on.
        :param module: name of the collection, you may want to name it base on site's module name, like twitter.user
        :param dict_params: task parameters
        :param uuid: task id
        :return:
        """
        task = {
            'uuid': uuid or gen_uuid(),
            'domain': domain,
            'module': module,
            'params': params
        }
        try:
            self.__task_queue.put(task, timeout=1)
        except Full:
            print 'Task queue is full.'
            return
        else:
            self.__dbm.update_task_basics(task)
            self.__dbm.update_task_status_queueing(task)

    def list_task_status(self, verbal=False):
        task_statuss = self.__dbm.list_task_status()
        if verbal:
            for task_status in task_statuss:
                uuid = task_status['uuid']
                status_text = self.__dbm.STATUS_TEXT[str(task_status['code'])]
                print 'Task', uuid, status_text
        else:
            return task_statuss

    def list_task_unfinished(self, verbal=False):
        tasks = self.__dbm.list_task_unfinished()
        if verbal:
            for task in tasks:
                uuid = task['uuid']
                status_text = self.__dbm.STATUS_TEXT[str(task['status']['code'])]
                task.pop('status')
                print 'Task', uuid, status_text
                print json.dumps(task, indent=4)
        else:
            for task in tasks:
                task.pop('status')
        return tasks

    def sleep_forever(self):
        while True:
            sleep(1)

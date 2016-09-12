from Queue import Full
from Queue import Queue
from utils import gen_uuid
from multiprocessing.managers import BaseManager as QueueManager


class BaseTaskQueue(object):
    def __init__(self):
        pass


class TaskQueueServer(BaseTaskQueue):
    def __init__(self, queue_password, queue_port, queue_size):
        super(TaskQueueServer, self).__init__()

        # Setup task queue
        task_queue = Queue(maxsize=queue_size)

        # Register task queue
        QueueManager.register('get_task_queue', callable=lambda: task_queue)

        # Bind localhost
        self.__manager = QueueManager(address=('', queue_port), authkey=queue_password)
        self.__manager.start()
        self.__task_queue = self.__manager.get_task_queue()
        print '[Task Queue Server] At 127.0.0.1:', queue_port, ', queue size is', queue_size

    def shutdown(self):
        print '[Task Queue Server] Shutting down'
        self.__manager.shutdown()

    def load_unfinished_tasks(self, dbm):
        tasks = dbm.list_unfinished_task_basics()
        for task in tasks:
            try:
                self.put_task(**task)
            except Full:
                print 'Task queue is full.'
                continue
            else:
                dbm.update_task_basics(task)
                dbm.update_task_status_queueing(task)
        print '[Task Queue Server] Load', len(tasks), 'unfinished tasks'

    @classmethod
    def print_unfinished_tasks(cls, dbm):
        dbm.list_unfinished_task_basics(verbal=True)

    def put_task(self, domain, module, params, uuid=None):
        """
        Put a task into queue
        :param domain: Domain name indicating which db name mongo will use and prefix redis key will use,
            you may want to name base on which site its crawling on.
        :param module: name of the collection, you may want to name it base on site's module name, like twitter.user
        :param params: task parameters
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
            raise


class TaskQueueClient(BaseTaskQueue):
    def __init__(self, queue_password, queue_addr='127.0.0.1', queue_port=0):
        super(TaskQueueClient, self).__init__()

        # Register task queue
        QueueManager.register('get_task_queue')

        # Bind localhost
        manager = QueueManager(address=(queue_addr, queue_port), authkey=queue_password)
        manager.connect()

        # Get queue from manager
        self.__task_queue = manager.get_task_queue()

    def get_task(self):
        task = self.__task_queue.get()  # will block if no tasks
        self.__task_queue.task_done()
        return task

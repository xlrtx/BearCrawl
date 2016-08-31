from multiprocessing.managers import BaseManager
from Queue import Queue
from Queue import Full


class QueueManager(BaseManager):
    pass


class TestWorker:
    def __init__(self, queue_password, queue_addr='127.0.0.1', queue_port=50000):
        # Register task queue
        QueueManager.register('get_task_queue')
        QueueManager.register('get_result_queue')

        # Bind localhost
        manager = QueueManager(address=(queue_addr, queue_port), authkey=queue_password)
        manager.connect()

        # Get queue from manager
        self.task_queue = manager.get_task_queue()
        self.result_queue = manager.get_result_queue()


class TestMaster:
    def __init__(self, queue_password, queue_port=50000, queue_size=3):
        # Setup task queue
        task_queue = Queue(maxsize=queue_size)
        result_queue = Queue(maxsize=queue_size)

        # Register task queue
        QueueManager.register('get_task_queue', callable=lambda: task_queue)
        QueueManager.register('get_result_queue', callable=lambda: result_queue)

        # Bind localhost
        manager = QueueManager(address=('', queue_port), authkey=queue_password)
        manager.start()

        # Get queue from manager
        self.task_queue = manager.get_task_queue()
        self.result_queue = manager.get_result_queue()

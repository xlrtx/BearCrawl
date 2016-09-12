from bearcrawl import BaseWorker
from time import sleep
import string
import random
import json


class Worker(BaseWorker):
    def __init__(self):
        super(Worker, self).__init__('grrrr', queue_port=34556)

    def __gen_str(self, size, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def __test_read_progress(self, worker_id):
        print worker_id, 'reading progress'
        progress = self.read_progress('progress')
        if progress:
            print progress
        else:
            print 'No Progress.'

    def __test_update_progress(self, worker_id):
        print worker_id, 'updating progress'
        self.update_progress({'progress': 3})

    def __test_save_dump(self, worker_id):
        print worker_id, 'saving dump'
        self.save_dump([{'data': 1}, {'data': 2}])

    def task_handler(self, uuid, domain, module, params):
        worker_id = 'worker ' + self.__gen_str(4)

        # Print task
        print worker_id, 'got task, task is'
        print json.dumps({'uuid': uuid, 'domain': domain, 'module': module, 'params': params}, indent=4)

        # Read progress
        self.__test_read_progress(worker_id)

        # Update progress
        self.__test_update_progress(worker_id)

        # Read progress
        self.__test_read_progress(worker_id)

        # Save dump
        self.__test_save_dump(worker_id)

        print 'worker working...'
        sleep(10)

        print 'worker finish'

w = Worker()
w.start()

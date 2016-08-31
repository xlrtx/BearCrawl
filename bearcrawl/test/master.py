from bearcrawl import BaseMaster
import string
import random
from time import sleep


class Master(BaseMaster):
    def __init__(self):
        super(Master, self).__init__('grrrr', queue_size=3)

    def __gen_str(self, size, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def __test_list_task_status(self):
        print '=== TEST LIST TASK STATUS ==='
        self.list_task_status(verbal=True)

    def __test_list_task_unfinished(self):
        print '=== TEST LIST TASK UNFINISHED ==='
        self.list_task_unfinished(verbal=True)

    def __test_put_task(self):
        print '=== TEST PUT TASK ==='
        domain = self.__gen_str(4)
        module = self.__gen_str(3)
        dict_params = {
            'a': 10,
            'b': 11
        }
        self.put_task(domain, module, dict_params)

    def test(self):
        self.__test_list_task_status()
        self.__test_list_task_unfinished()
        self.__test_put_task()
        self.__test_put_task()
        self.__test_list_task_status()
        self.__test_list_task_unfinished()
        self.__test_put_task()
        self.__test_put_task()
        self.__test_list_task_status()
        self.__test_list_task_unfinished()

    def halt(self):
        while True:
            sleep(5)


m = Master()
m.test()
m.halt()

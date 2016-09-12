from bearcrawl import BaseMaster
from bearcrawl import RPCProxy
from xmlrpclib import Fault
import threading
import string
import random
import json


class Master(BaseMaster):
    def __init__(self):
        super(Master, self).__init__('grrrr', queue_port=34556, queue_size=3)


def __gen_str(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def test_list_task_status(rpc_proxy):
    print '=== TEST LIST TASK STATUS ==='
    try:
        return rpc_proxy.list_task_status()
    except Fault as err:
        print 'Fault code', err.faultCode
        print 'Fault string', err.faultString


def test_list_task_unfinished(rpc_proxy):
    print '=== TEST LIST TASK UNFINISHED ==='
    try:
        return rpc_proxy.list_unfinished_task_basics()
    except Fault as err:
        print 'Fault code', err.faultCode
        print 'Fault string', err.faultString


def test_put_task(rpc_proxy):
    print '=== TEST PUT TASK ==='
    domain = 'TEST'
    module = __gen_str(3)
    dict_params = {
        'a': 10,
        'b': 11
    }
    try:
        return rpc_proxy.put_task(domain, module, dict_params)
    except Fault as err:
        print 'Fault code', err.faultCode
        print 'Fault string', err.faultString


def json_print(obj):
    print json.dumps(obj, indent=4)


def test():
    rpc_proxy = RPCProxy()
    json_print(test_list_task_status(rpc_proxy))
    json_print(test_list_task_unfinished(rpc_proxy))
    json_print(test_put_task(rpc_proxy))
    json_print(test_put_task(rpc_proxy))
    json_print(test_list_task_status(rpc_proxy))
    json_print(test_list_task_unfinished(rpc_proxy))
    json_print(test_put_task(rpc_proxy))
    json_print(test_put_task(rpc_proxy))
    json_print(test_list_task_status(rpc_proxy))
    json_print(test_list_task_unfinished(rpc_proxy))


tim = threading.Timer(3, test)
tim.setDaemon(True)
tim.start()
m = Master()

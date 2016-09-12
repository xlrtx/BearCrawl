from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import redis
from pymongo.errors import PyMongoError


class DatabaseManager:
    CODE_FINISH = 0
    CODE_ERROR = -1
    CODE_WORKING = 1
    CODE_QUEUEING = 2

    STATUS_TEXT = {
        str(CODE_FINISH): 'FINISH',
        str(CODE_ERROR): 'ERROR',
        str(CODE_WORKING): 'WORKING',
        str(CODE_QUEUEING): 'QUEUEING'
    }

    def __init__(self, mongo_conn=None, redis_client=None):
        """
        :param mongo_conn: MongoDB connection, will use default localhost if not given
        :param redis_client: Redis client, will use default localhost if not given
        :return:
        """
        if mongo_conn:
            self.__mongo_conn = mongo_conn
        else:
            self.__mongo_conn = MongoClient()
        if redis_client:
            self.__redis_client = redis_client
        else:
            self.__redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

        self._db_task = self.__mongo_conn['task']
        self.__col_task = self._db_task['task']

    ''' Mongodb Dump '''

    def save_dump(self, task, data):
        """
        Used by worker
        batch insert crawled data under domain.name_col
        :param task: task got from queue.
        :param data: dump data in array
        :return: number of duplicates
        """
        domain = task['domain']
        module = task['module']
        db = self.__mongo_conn[domain]
        col = db[module]
        try:
            col.insert_many(data, ordered=False)
        except BulkWriteError, bwe:
            inserted = bwe.details['nInserted']
            duplicated = len(data) - inserted
            return duplicated
        else:
            return 0

    '''
    Mongodb Task Data Structure
        task = {
            '_id': '',
            'domain': '',
            'module': '',
            'params': '',
            'status': {
                'code': 0,
                'detail': {}
            },
            'progress': {}
        }
    '''

    ''' Mongodb Task Basics '''

    def update_task_basics(self, task):
        """
        Used by worker
        save task basics, including uuid, domain, module and params

        Format of task from message queue
        task = {
            'uuid': '',
            'domain': '',
            'module': '',
            'params': ''
        }
        :param task:
        :return:
        """
        col = self.__col_task
        formatted = {
            '_id': task['uuid'],
            'domain': task['domain'],
            'module': task['module'],
            'params': task['params']
        }
        try:
            col.update({'_id': formatted['_id']}, {'$set': formatted}, upsert=True)
        except PyMongoError:
            raise

    ''' Mongodb Task Progress'''

    def update_task_progress(self, task, dict_progress):
        """
        Used by worker
        save crawling progress, so it can continue after crash
        :param task: task got from queue.
        :param dict_progress: progress detail
        :return:
        """
        col = self.__col_task
        uuid = task['uuid']
        col.update({'_id': uuid}, {'$set': {'progress': dict_progress}}, upsert=True)

    def read_task_progress(self, task):
        """
        Used by worker
        read crawling progress, so it can continue after crash
        :param task: task got from queue.
        :return: progress detail
        """
        col = self.__col_task
        uuid = task['uuid']
        doc = col.find_one({'_id': uuid})
        if doc and 'progress' in doc:
            return doc['progress']
        else:
            return None

    ''' Mongodb Task Status '''

    def update_task_status(self, task, code, detail=None):
        """
        Used by worker
        Update current task status, for master to read.
        :param task: task got from queue.
        :param code: status code, see top
        :param detail: custom detail
        :return:
        """
        col = self.__col_task
        uuid = task['uuid']
        detail = detail or {}
        try:
            col.update({'_id': uuid}, {'$set': {'status': {'code': code, 'detail': detail}}}, upsert=True)
        except PyMongoError:
            raise

    def update_task_status_queueing(self, task, detail=None):
        """
        Used by master
        Update task status to queueing after put it into queue.
        :param task: task got from queue.
        :param detail: custom detail
        :return:
        """
        self.update_task_status(task, self.CODE_QUEUEING, detail=detail)

    def update_task_status_working(self, task, detail=None):
        """
        Used by master
        Update current task status.
        :param task: task got from queue.
        :param detail: custom detail
        :return:
        """
        self.update_task_status(task, self.CODE_WORKING, detail=detail)

    def update_task_status_finish(self, task, detail=None):
        """
        Used by worker
        Update current task status, for master to read.
        :param task: task got from queue.
        :param detail: custom detail
        :return:
        """
        self.update_task_status(task, self.CODE_FINISH, detail=detail)

    def update_task_status_error(self, task, detail=None):
        """
        Used by worker
        Update current task status, for master to read.
        :param task: task got from queue.
        :param detail: custom detail
        :return:
        """
        self.update_task_status(task, self.CODE_ERROR, detail=detail)

    def read_task_status(self, task):
        """
        Used by master
        Read one task status
        :param task: task got from queue.
        :return: one task status
        """
        col = self.__col_task
        uuid = task['uuid']
        doc = col.find_one({'_id': uuid})
        status = doc['status']
        out = {
            'uuid': uuid,
            'code': status['code'],
            'detail': status['detail']
        }
        return out

    def list_task_status(self):
        """
        Used by master
        Read all task statuses
        :return: all task statuses
        """
        col = self.__col_task
        try:
            docs = col.find({}, {'_id': 1, 'status': 1})
        except PyMongoError:
            raise
        out = []
        for doc in docs:
            uuid = doc['_id']
            status = doc['status']
            out += [{
                'uuid': uuid,
                'code': status['code'],
                'detail': status['detail']
            }]
        return out

    def list_unfinished_task_basics(self):
        """
        Used by master
        :return: All unfinished tasks (including task basics)
        """
        col = self.__col_task
        try:
            docs = col.find({'status.code': {'$not': {'$eq': 0}}}, {'progress': 0})
        except PyMongoError:
            raise
        out = []
        for doc in docs:
            doc['uuid'] = doc.pop('_id')
            doc.pop('status', None)
            doc.pop('progress', None)
            out += [doc]
        return out

    ''' Redis Filter '''

    def update_filter(self, task, filter_name, items):
        """
        Used by worker
        Update filter set
        :param task: task got from queue.
        :param filter_name: name of filter
        :param items: items add to set
        :return:
        """
        client = self.__redis_client
        domain = task['domain']
        module = task['module']
        name_key = 'filter_' + domain + '_' + module + '_' + filter_name
        client.sadd(name_key, *items)

    def filter(self, task, filter_name, items):
        """
        Used by worker
        Filter a list of items
        :param task: task got from queue.
        :param filter_name: name of filter
        :param items: items to filter
        :return: filtered items
        """
        client = self.__redis_client
        domain = task['domain']
        module = task['module']
        name_key = 'filter_' + domain + '_' + module + '_' + filter_name
        items = [i for i in items if not client.sismember(name_key, i)]
        return items

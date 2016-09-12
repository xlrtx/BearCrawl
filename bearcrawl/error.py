class BearCrawlException(Exception):
    def __init__(self, *args, **kwargs):
        super(BearCrawlException, self).__init__(*args, **kwargs)


class TaskHandlerNotImplemented(BearCrawlException):
    """ Need implement task handler """


class RPCQueueFull(BearCrawlException):
    """ Queue is full """


class RPCDBError(BearCrawlException):
    """ DB Error """

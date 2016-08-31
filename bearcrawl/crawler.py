import sys
import requests
import traceback
import functools
from itertools import chain
from time import sleep
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

# TODO add proxy manager
# TODO map don't respond to ctrl-c interrupt
# TODO make filter func a compulsory field
RETRY_TIMES = 10


def getn(urls, filter_func, map_reduce=True, ordered=False, chain_results=False, threads=None, **kwargs):
    return requestn("GET", urls, filter_func, map_reduce=map_reduce, ordered=ordered,
                    threads=threads, chain_results=chain_results, **kwargs)


def postn(urls, filter_func, map_reduce=True, ordered=False, chain_results=False, threads=None, **kwargs):
    return requestn("POST", urls, filter_func, map_reduce=map_reduce, ordered=ordered,
                    threads=threads, chain_results=chain_results, **kwargs)


def requestn(method, urls, filter_func, map_reduce=True, ordered=False,
             chain_results=False, threads=None, **kwargs):
    """
    Request multiple urls
    :param method: request method
    :param urls: url array
    :param filter_func: filter function to parse a single response, return filtered data
    :param map_reduce: use multi-threaded map reduce by default, if not, single-threaded
    :param ordered: not ordered (map_async) by default, if not, use map.
    :param chain_results: if return an array of arrays, join arrays together
    :param threads: number of threads
    :param kwargs: kwargs passed to requests
    :return: filtered results
    """
    if map_reduce:
        threads = threads if threads else multiprocessing.cpu_count()
        pool = ThreadPool(threads)
        if ordered:
            results = pool.map(functools.partial(__crawl, method, filter_func, **kwargs), urls)
        else:
            results = pool.map_async(functools.partial(__crawl, method, filter_func, **kwargs), urls)\
                .get(99999)
        pool.close()
        pool.join()
        if chain_results:
            results = list(chain.from_iterable(results))

    else:
        results = []
        for url in urls:
            data = __crawl(method, filter_func, url, **kwargs)
            results = results + data if chain_results else results + [data]

    return results


def __retry(func):
    # TODO find a better exception representation
    def inner(*args, **kwargs):
        try_count = 1
        while True:
            try:
                return func(*args, **kwargs)
            except Exception, e:
                try_count += 1
                if try_count > RETRY_TIMES:
                    exc_info = sys.exc_info()
                    traceback.print_exception(*exc_info)
                    print '====Args===='
                    for arg in args:
                        print arg
                    print '====kwArgs===='
                    for k, v in kwargs.iteritems():
                        print k, ':', v
                    print '============='
                    raise Exception('Retry Failed - ' + e.__class__.__name__)
                sleep(1)

    return inner


@__retry
def __crawl(method, filter_func, url, **kwargs):
    r = requests.request(method, url, **kwargs)
    if filter_func:
        return filter_func(r)
    return r

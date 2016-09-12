import sys
import random
import requests
import traceback
import functools
from itertools import chain
from time import sleep
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

RETRY_TIMES = 10
PROXIES = None


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
            results = pool.map_async(functools.partial(__crawl, method, filter_func, **kwargs), urls) \
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
    if PROXIES and 'proxies' not in kwargs:
        proxies = __pick_one_proxy()
        r = requests.request(method, url, proxies=proxies, **kwargs)
    else:
        r = requests.request(method, url, **kwargs)
    if filter_func:
        return filter_func(r)
    return r


def set_proxies(dict_proxies):
    """
    Set up proxies used for requests module
    same as proxies filed passed to requests, but instead an array of proxies for each key
    need install requests[socks] for socks proxies
    after set, each request will select a random proxy to use
    dict_proxies = {
        'http': ['http://user:pass@10.10.1.10:3128/', 'http://10.10.1.10:3128'],
        'https': ['http://user:pass@10.10.1.10:3128/', 'http://10.10.1.10:3128'],
        'scheme://hostname': ['socks5://user:pass@host:port']
    }
    :param dict_proxies:
    :return:
    """
    global PROXIES
    PROXIES = dict_proxies


def __pick_one_proxy():
    if PROXIES is None:
        return None
    picked = {}
    for k, v in PROXIES.iteritems():
        picked[k] = random.choice(v)
    return picked

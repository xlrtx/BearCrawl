from error import InvalidParamArrSize
from error import InvalidThreadSize
import sys
import random
import requests
import traceback
import functools
import collections
from itertools import izip
from itertools import chain
from time import sleep
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

RETRY_TIMES = 10
PROXIES = None


# TODO if size too big, use batch
# TODO try kwargs in args_arr

def get(url, filter_func, args=None, **kwargs):
    return request('GET', url, filter_func, args, **kwargs)


def post(url, filter_func, args=None, **kwargs):
    return request('POST', url, filter_func, args, **kwargs)


def request(method, url, filter_func, args=None, **kwargs):
    if not args:
        args = []
    return __crawl(method, filter_func, (url, args), **kwargs)


def getn(urls, filter_func, args_arr=None, chain_results=False, threads=None, **kwargs):
    return requestn("GET", urls, filter_func, args_arr=args_arr,
                    threads=threads, chain_results=chain_results, **kwargs)


def postn(urls, filter_func, args_arr=None, chain_results=False, threads=None, **kwargs):
    return requestn("POST", urls, filter_func, args_arr=args_arr,
                    threads=threads, chain_results=chain_results, **kwargs)


def requestn(method, urls, filter_func, args_arr=None, chain_results=False, threads=None, **kwargs):
    """
    Request multiple urls
    :param method: request method
    :param urls: url array
    :param filter_func: filter function to parse a single response, return filtered data
    :param args_arr: parameters passed in filter function appended after r object, must be same length as len(url)
    :param map_reduce: use multi-threaded map reduce by default, if not, single-threaded
    :param chain_results: if return an array of arrays, join arrays together
    :param threads: number of threads
    :param kwargs: kwargs passed to requests
    :return: filtered results
    """
    if not args_arr:
        count_urls = len(urls)
        args_arr = [[]] * count_urls
    else:
        if len(urls) != len(args_arr):
            raise InvalidParamArrSize

    if threads < 1:
        raise InvalidThreadSize

    if threads >= 1:
        threads = threads if threads else multiprocessing.cpu_count()
        pool = ThreadPool(threads)
        results = pool.map(functools.partial(__crawl, method, filter_func, **kwargs), izip(urls, args_arr))
        pool.close()
        pool.join()
        results = [each for each in results if each is not None]
        if chain_results:
            results = list(chain.from_iterable(results))

    else:
        # TODO Haven't tested fully
        results = []
        for url, args in izip(urls, args_arr):
            data = __crawl(method, filter_func, (url, args), **kwargs)
            if data is None:
                continue
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
                        print k, ' : ', v
                    print '============='
                    raise Exception('Retry Failed - ' + e.__class__.__name__)
                sleep(1)

    return inner


@__retry
def __crawl(method, filter_func, tup, **kwargs):
    url = tup[0]
    args = tup[1]
    if PROXIES and 'proxies' not in kwargs:
        proxies = __pick_one_proxy()
        r = requests.request(method, url, proxies=proxies, **kwargs)
    else:
        r = requests.request(method, url, **kwargs)

    if isinstance(args, collections.Sequence) and not isinstance(args, basestring):
        return filter_func(r, *args)
    else:
        return filter_func(r, args)


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

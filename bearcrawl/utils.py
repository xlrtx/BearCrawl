import uuid
import time

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def gen_uuid():
    return str(uuid.uuid1())


# %Y%m%d
def str_to_timestamp(ts_str, format_str):
    return int(time.mktime(time.strptime(ts_str, format_str)))

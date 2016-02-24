import gzip
import os
from collections import Iterator
from contextlib import contextmanager

import bson
from bson.objectid import ObjectId
from datashape import DataShape, Record, discover, var
from odo import Temp, convert, discover, odo, resource
from toolz import take


class BSON(object):
    """ Proxy for a JSON file
    Parameters
    ----------
    path : str
        Path to file on disk
    See Also
    --------
    JSONLines - Line-delimited JSON
    """
    canonical_extension = 'json'

    def __init__(self, path, **kwargs):
        self.path = path


@contextmanager
def bson_lines(path):
    """ Return lines of a bson-lines file
    Handles compression like gzip """
    def convert_id(lines):
        for line in lines:
            line['_id'] = str(line['_id'])
            yield line

    if path.split(os.path.extsep)[-1] == 'gz':
        f = gzip.open(path)
    else:
        f = open(path)

    try:
        yield convert_id(bson.decode_file_iter(f))
    finally:
        f.close()


# def bson_load(path, **kwargs):
#     """ Return data of a bson file
#     Handles compression like gzip """
#     with bson_lines(path) as lines:
#         return list(lines)


@resource.register('bson://.*\.bson(\.gz)?', priority=11)
def resource_bson(path, **kwargs):
    if 'bson://' in path:
        path = path[len('bson://'):]
    return BSON(path)


@discover.register(BSON)
def discover_bson(b, n=10, **kwargs):
    with bson_lines(b.path) as lines:
        data = list(take(n, lines))

    if len(data) < n:
        ds = discover(data)
    else:
        ds = var * discover(data).subshape[0]
    return ds
    # return date_to_datetime_dshape(ds)


@convert.register(list, (BSON, Temp(BSON)))
def bson_to_list(b, dshape=None, **kwargs):
    with bson_lines(b.path) as lines:
        return list(lines)


@convert.register(Iterator, (BSON, Temp(BSON)))
def bson_lines_to_iterator(b, **kwargs):
    return bson_lines(b.path, **kwargs)

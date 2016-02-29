from __future__ import absolute_import, division, print_function

import gzip
import os
import uuid
from collections import Iterator
from contextlib import contextmanager

import bson
from datashape import var
from odo import Temp, append, convert, discover, drop, resource
from odo.convert import ooc_types
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
    canonical_extension = 'bson'

    def __init__(self, path, **kwargs):
        self.path = path


@contextmanager
def bson_lines(path, **kwargs):
    """ Return lines of a bson-lines file
    Handles compression like gzip """
    def convert_id(lines):
        for line in lines:
            line.pop("_id", None)
            yield line

    if path.split(os.path.extsep)[-1] == 'gz':
        f = gzip.open(path)
    else:
        f = open(path)

    try:
        yield convert_id(bson.decode_file_iter(f))
    finally:
        f.close()


@resource.register('(bson://)?.*\.bson(\.gz)?')
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
def bson_to_iterator(b, **kwargs):
    with bson_lines(b.path, **kwargs) as bs:
        for line in bs:
            yield line


@append.register(BSON, object)
def object_to_bson(b, o, **kwargs):
    return append(b, convert(Iterator, o, **kwargs), **kwargs)


@append.register(BSON, (Iterator, list))
def iterator_to_bson(b, seq, dshape=None, **kwargs):
    lines = (bson.BSON.encode(item) for item in seq)

    # Open file
    if b.path.split(os.path.extsep)[-1] == 'gz':
        f = gzip.open(b.path, 'ab')
    else:
        f = open(b.path, 'a')

    for line in lines:
        f.write(line)

    f.close()

    return b


@convert.register(Temp(BSON), list)
def list_to_temporary_bson(data, **kwargs):
    fn = '.%s.bson' % uuid.uuid1()
    target = Temp(BSON)(fn)
    return append(target, data, **kwargs)


@drop.register(BSON)
def drop_bson(bs):
    if os.path.exists(bs.path):
        os.remove(bs.path)


ooc_types.add(BSON)

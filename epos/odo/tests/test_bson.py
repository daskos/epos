# coding: utf-8

from __future__ import absolute_import, division, print_function

import gzip
import os
from contextlib import contextmanager

import bson
from datashape import dshape
from epos.odo.bson import BSON
from odo import append, convert, discover, drop, odo, resource
from odo.temp import Temp, _Temp
from odo.utils import tmpfile


@contextmanager
def bson_file(data):
    with tmpfile('.bson') as fn:
        with open(fn, 'wb') as f:
            for item in data:
                f.write(bson.BSON.encode(item))
        yield fn


dat = [{'name': 'Alice', 'amount': 100},
       {'name': 'Bob', 'amount': 200}]


def test_discover_bson():
    with bson_file(dat) as fn:
        b = BSON(fn)
        assert discover(b) == discover(dat)


def test_resource():
    with tmpfile('bson') as fn:
        assert isinstance(resource('bson://' + fn), BSON)
        assert isinstance(
            resource(fn, expected_dshape=dshape('var * {a: int}')),
            BSON)


def test_resource_guessing():
    with bson_file(dat) as fn:
        assert isinstance(resource(fn), BSON)


def test_append_bson():
    with tmpfile('bson') as fn:
        b = BSON(fn)
        append(b, dat)
        assert convert(list, b) == dat


def test_convert_bson_list():
    with bson_file(dat) as fn:
        b = BSON(fn)
        assert convert(list, b) == dat


# def test_datetimes():
#    from odo import into
#    import numpy as np
#    data = [{'a': 1, 'dt': datetime.datetime(2001, 1, 1)},
#            {'a': 2, 'dt': datetime.datetime(2002, 2, 2)}]
#    with tmpfile('json') as fn:
#        j = JSONLines(fn)
#        append(j, data)

#        assert str(into(np.ndarray, j)) == str(into(np.ndarray, data))


# def test_multiple_jsonlines():
#     a, b = '_test_a1.json', '_test_a2.json'
#     try:
#         with ignoring(OSError):
#             os.remove(a)
#         with ignoring(OSError):
#             os.remove(b)
#         with open(a, 'w') as f:
#             json.dump(dat, f)
#         with open(b'_test_a2.json', 'w') as f:
#             json.dump(dat, f)
#         r = resource('_test_a*.json')
#         result = convert(list, r)
#         assert len(result) == len(dat) * 2
#     finally:
#         with ignoring(OSError):
#             os.remove(a)
#         with ignoring(OSError):
#             os.remove(b)


def test_read_gzip():
    with tmpfile('bson.gz') as fn:
        f = gzip.open(fn, 'wb')
        for item in dat:
            f.write(bson.BSON.encode(item))
        f.close()
        b = BSON(fn)
        assert convert(list, b) == dat


def test_write_gzip():
    with tmpfile('bson.gz') as fn:
        b = BSON(fn)
        append(b, dat)

        assert convert(list, b) == dat


def test_resource_gzip():
    with tmpfile('bson.gz') as fn:
        assert isinstance(resource(fn), BSON)
        assert isinstance(resource('bson://' + fn), BSON)


def test_convert_to_temp_bson():
    bs = convert(Temp(BSON), dat)
    assert isinstance(bs, BSON)
    assert isinstance(bs, _Temp)

    assert convert(list, bs) == dat


def test_drop():
    with tmpfile('bson') as fn:
        bs = BSON(fn)
        append(bs, dat)

        assert os.path.exists(fn)
        drop(bs)
        assert not os.path.exists(fn)


def test_missing_to_csv():
    data = [dict(a=1, b=2), dict(a=2, c=4)]
    with tmpfile('.bson') as fn:
        bs = BSON(fn)
        bs = odo(data, bs)

        with tmpfile('.csv') as csvf:
            csv = odo(bs, csvf)
            with open(csv.path, 'rt') as f:
                result = f.read()

    expected = 'a,b,c\n1,2.0,\n2,,4.0\n'
    assert result == expected

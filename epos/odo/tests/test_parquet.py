from __future__ import absolute_import, division, print_function

import pandas as pd
import pytest
from epos.odo.parquet import Parquet, resource
from odo import HDFS, odo

pyspark = pytest.importorskip('pyspark')
py4j = pytest.importorskip('py4j')
pywebhdfs = pytest.importorskip('pywebhdfs')


data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3]]


df = pd.DataFrame(data, columns=['name', 'amount', 'id'])


def test_resource():
    assert isinstance(resource('foo.parquet'), Parquet)
    assert isinstance(resource('/path/to/foo.parquet'), Parquet)


def test_hdfs_resource():
    auth = {'user': 'hdfs', 'port': 14000, 'host': 'hostname'}
    tmp = resource('hdfs://foo.parquet', **auth)
    assert isinstance(tmp, HDFS(Parquet))
    assert tmp.path == 'foo.parquet'


def test_convert_df_to_parquet_locally(sqlctx, tmpdir):
    path = str(tmpdir.join("output.parquet"))
    sdf = odo(df, sqlctx)
    odo(sdf, path)  # write local parquet file
    sdf_ = sqlctx.read.parquet('file://' + path)

    assert sdf_.collect() == sdf.collect()


def test_convert_df_to_parquet_hdfs(sqlctx, tmpdir):
    path = 'hdfs:///tmp/test.parquet'
    sdf = odo(df, sqlctx)
    odo(sdf, path, host='localhost', user='test')  # write parquet file to HDFS
    sdf_ = sqlctx.read.parquet(path)

    assert sdf_.collect() == sdf.collect()

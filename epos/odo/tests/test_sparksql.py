from __future__ import print_function, absolute_import, division

import os
import pytest
from odo.utils import tmpfile

pyspark = pytest.importorskip('pyspark')
py4j = pytest.importorskip('py4j')

from odo import odo, convert
from odo import HDFS
from epos.odo import Parquet


data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3],
        ['Anna', 150.0, 5],
        ['Mads', 20.0, 4],
        ['Josh', 80.0, 1]]


@pytest.fixture(scope='module')
def sdf(sc):
    rdd = sc.parallelize(data, 2)
    return rdd.toDF()


# @pytest.yield_fixture
# def tmpdir_hdfs(hdfs, ext=''):
#     fn = '/tmp/' + str(uuid.uuid1())
#     if ext:
#         fn = fn + '.' + ext.lstrip('.')

#     try:
#         yield fn
#     finally:
#         hdfs.delete_file_dir(fn)


def test_append_sparkdf_to_parquet(sqlctx, sdf):
    with tmpfile('.parquet') as path:
        local_path = 'file://{path}'.format(path=path)
        res = odo(sdf, Parquet(path))  # write local parquet file
        sdf_ = sqlctx.read.parquet(local_path)

        assert os.path.exists(path)
        assert sdf_.collect() == sdf.collect()


def test_append_sparkdf_to_parquet_hdfs(sqlctx, hdfs, sdf):
    path = '/tmp/test.parquet'
    hdfs_path = 'hdfs://{host}/{path}'.format(host=hdfs.host,
                                              path=path.lstrip('/'))

    # write parquet file to HDFS
    res = odo(sdf, HDFS(Parquet)(path, hdfs=hdfs))
    sdf_ = sqlctx.read.parquet(hdfs_path)
    assert sdf_.collect() == sdf.collect()

from __future__ import print_function, absolute_import, division

import os
import pytest

from odo import odo, convert, HDFS
from odo.utils import tmpfile
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


@pytest.fixture(scope='module')
def sdfc(sc):
    d = [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}]
    rdd = sc.parallelize(d, 2)
    return rdd.toDF()


def test_append_sparkdf_to_parquet(sqlctx, sdf):
    with tmpfile('.parquet') as path:
        local_path = 'file://{path}'.format(path=path)
        res = odo(sdf, Parquet(path))  # write local parquet file
        sdf_ = sqlctx.read.parquet(local_path)

        assert os.path.exists(path)
        assert set(sdf_.collect()) == set(sdf.collect())


def test_append_sparkdf_to_parquet_hdfs(sqlctx, hdfs, sdf):
    path = '/tmp/test.parquet'
    hdfs_path = 'hdfs://{host}/{path}'.format(host=hdfs.host,
                                              path=path.lstrip('/'))

    # write parquet file to HDFS
    res = odo(sdf, HDFS(Parquet)(path, hdfs=hdfs), mode='append')
    sdf_ = sqlctx.read.parquet(hdfs_path)
    assert set(sdf_.collect()) == set(sdf.collect())


@pytest.mark.skip(reason='spark-connector incompatibility with spark 2.0')
def test_append_sparksql_dataframe_to_cassandra(sqlctx, sdfc, cass):
    path = 'cql://localhost:9160/testks::testtable'
    odo(sdfc, path)
    res = odo(path, sqlctx)
    assert res.collect() == sdfc.collect()

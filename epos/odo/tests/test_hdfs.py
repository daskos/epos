from __future__ import absolute_import, division, print_function

import pytest
from odo import resource, HDFS
from epos.odo import Parquet


def test_hdfs_parquet_reource(hdfs):
    res = resource('hdfs://foo.parquet', hdfs=hdfs)
    assert isinstance(res, HDFS(Parquet))
    assert res.path == 'foo.parquet'

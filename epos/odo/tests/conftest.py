import glob
import os
import shutil
import sys

import pytest


hdfs_host = os.environ.get('HDFS_TEST_HOST')
spark_home = os.environ['SPARK_HOME']
spark_python = os.path.join(spark_home, 'python')
py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
sys.path[:0] = [spark_python, py4j]


@pytest.yield_fixture(scope='module')
def sc():
    pytest.importorskip('pyspark')
    from pyspark import SparkContext
    with SparkContext(appName="epos-tests", master="local[*]") as sc:
        log4j = sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
        yield sc


@pytest.fixture(scope='module')
def sqlctx(sc):
    pytest.importorskip('pyspark')
    from pyspark.sql import SQLContext
    return SQLContext(sc)


@pytest.fixture(scope='module')
def hdfs():
    pywebhdfs = pytest.importorskip('pywebhdfs')
    pytestmark = pytest.mark.skipif(hdfs_host is None,
                                    reason='No HDFS_TEST_HOST envar defined')

    from pywebhdfs.webhdfs import PyWebHdfsClient
    return PyWebHdfsClient(host=hdfs_host, user_name='hdfs')

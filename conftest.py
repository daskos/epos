import glob
import os
import shutil
import sys

import pytest


hdfs_host = os.environ.get('HDFS_HOST')
zookeeper_host = os.environ.get('ZOOKEEPER_HOST')
spark_home = os.environ['SPARK_HOME']
spark_python = os.path.join(spark_home, 'python')
cassandra_host = os.environ.get('CASSANDRA_TEST_HOST')
cassandra_port = os.environ.get('CASSANDRA_TEST_PORT')
py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
sys.path[:0] = [spark_python, py4j]


@pytest.yield_fixture
def zk():
    pytest.importorskip('kazoo')
    pytestmark = pytest.mark.skipif(zookeeper_host is None,
                                    reason='No ZOOKEEPER_HOST envar defined')

    from kazoo.client import KazooClient
    zk = KazooClient(hosts=zookeeper_host)
    try:
        zk.start()
        yield zk
    finally:
        zk.delete('/epos', recursive=True)
        zk.stop()


@pytest.yield_fixture(scope='module')
def sc():
    pytest.importorskip('pyspark')
    pytestmark = pytest.mark.skipif(spark_home is None,
                                    reason='No SPARK_HOME envar defined')

    from pyspark import SparkContext, SparkConf

    conf = SparkConf()
    conf.setAppName('epos-tests')
    conf.setMaster('local[*]')
    conf.set('spark.cassandra.connection.host', cassandra_host)
    conf.set('spark.cassandra.connection.port', cassandra_port)

    with SparkContext(conf=conf) as sc:
        log4j = sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
        yield sc


@pytest.fixture(scope='module')
def sqlctx(sc):
    pytest.importorskip('pyspark')
    from pyspark.sql import SQLContext
    return SQLContext(sc)


@pytest.fixture(scope='module')
def cass(sc):
    pytest.importorskip('cassandra')
    from cassandra.cluster import Cluster

    c = Cluster(cassandra_host.split(','), port=int(cassandra_port)).connect()
    c.execute(
        "CREATE KEYSPACE testks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    c.set_keyspace('testks')
    c.execute("CREATE TABLE testtable (a int, b int, PRIMARY KEY (a, b))")
    return cass


@pytest.fixture(scope='module')
def hdfs():
    pywebhdfs = pytest.importorskip('pywebhdfs')
    pytestmark = pytest.mark.skipif(hdfs_host is None,
                                    reason='No HDFS_HOST envar defined')

    from pywebhdfs.webhdfs import PyWebHdfsClient
    return PyWebHdfsClient(host=hdfs_host, user_name='hdfs')

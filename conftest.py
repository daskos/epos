import os
import pytest
import logging

logging.basicConfig(level=logging.ERROR,
                    format='%(relativeCreated)6d %(threadName)s %(message)s')

spark_home = os.environ.get('SPARK_HOME')
hdfs_host = os.environ.get('HDFS_HOST')
zookeeper_host = os.environ.get('ZOOKEEPER_HOST')
cassandra_host = os.environ.get('CASSANDRA_HOST')
kafka_host = os.environ.get('KAFKA_HOST')


@pytest.yield_fixture
def zk():
    pytest.importorskip('kazoo')
    pytest.mark.skipif(zookeeper_host is None,
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
    pytest.mark.skipif(spark_home is None,
                       reason='No SPARK_HOME envar defined')

    from pyspark import SparkContext, SparkConf

    conf = SparkConf()
    conf.setAppName('epos-tests')
    conf.setMaster('local[2]')

    # try:
    #     chost, cport = cassandra_host.split(':')
    #     conf.set('spark.cassandra.connection.host', chost)
    #     conf.set('spark.cassandra.connection.port', cport)
    #     # spark cassandra packege must be added to spark-defaults.conf
    # except:
    #     pass

    with SparkContext(conf=conf) as sc:
        log4j = sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
        yield sc


@pytest.fixture(scope='module')
def sqlctx(sc):
    pytest.importorskip('pyspark')
    from pyspark.sql import SQLContext
    return SQLContext(sc)


# @pytest.yield_fixture(scope='module')
# def cass(sc):
#     pytest.importorskip('cassandra')
#     from cassandra.cluster import Cluster

#     chost, cport = cassandra_host.split(':')
#     c = Cluster([chost], port=int(cport)).connect()

#     try:
#         c.execute("CREATE KEYSPACE testks WITH REPLICATION = "
#                   "{'class': 'SimpleStrategy', 'replication_factor': 1}")
#         c.set_keyspace('testks')
#         c.execute("CREATE TABLE testtable (a int, b int, PRIMARY KEY (a, b))")
#         yield c
#     finally:
#         c.execute("DROP KEYSPACE testks")
#         c.shutdown()


@pytest.fixture(scope='module')
def hdfs():
    pytest.importorskip('pywebhdfs')
    pytest.mark.skipif(hdfs_host is None,
                       reason='No HDFS_HOST envar defined')

    from pywebhdfs.webhdfs import PyWebHdfsClient
    return PyWebHdfsClient(host=hdfs_host, user_name='hdfs')


@pytest.fixture(scope='module')
def kafka():
    pytest.importorskip('pykafka')
    pytest.mark.skipif(kafka_host is None,
                       reason='No KAFKA_HOST envar defined')
    from pykafka import KafkaClient
    return KafkaClient(hosts=kafka_host)

import glob
import os
import shutil
import sys

import pytest

spark_home = os.environ['SPARK_HOME']
spark_python = os.path.join(spark_home, 'python')
py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
sys.path[:0] = [spark_python, py4j]


@pytest.fixture(scope="session")
def sc():
    from pyspark import SparkContext
    sc = SparkContext(appName="epos-tests", master="local[*]")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    return sc


@pytest.fixture(scope='session')
def sqlctx(sc):
    pytest.importorskip('pyspark')
    from pyspark.sql import SQLContext
    return SQLContext(sc)

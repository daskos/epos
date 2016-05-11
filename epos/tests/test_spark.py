import pytest

pytest.importorskip('pyspark')

from epos import spark
from pyspark import SparkContext
from pyspark.sql import SQLContext


@pytest.fixture(scope='module')
def options_proxy():
    def test_job(sc, sql):
        return dict(sc._conf.getAll())
    return test_job


@pytest.fixture(scope='module')
def decorated_sum():
    @spark
    def job(sc, sql, lst):
        rdd = sc.parallelize(lst)
        return rdd.sum()

    return job


@pytest.fixture(scope='module')
def curried_sum():
    def job(sc, sql, lst):
        rdd = sc.parallelize(lst)
        return rdd.sum()

    return spark(job)


def test_argument_injection():
    sc, sql = spark(lambda sc, sql: (sc, sql))()

    assert isinstance(sc, SparkContext)
    assert isinstance(sql, SQLContext)


def test_app_name(options_proxy):
    opts = spark(options_proxy)()
    assert opts.get('spark.app.name') == 'test_job'

    opts = spark(options_proxy, name='test_name')()
    assert opts.get('spark.app.name') == 'test_name'


def test_coarse_mode(options_proxy):
    opts = spark(options_proxy)()
    assert opts.get("spark.mesos.coarse") == 'False'

    opts = spark(options_proxy, coarse=10)()
    assert opts.get('spark.mesos.coarse') == 'True'
    assert opts.get('spark.coarse.max') == '10'


def test_docker_executor(options_proxy):
    opts = spark(options_proxy)()
    assert opts.get('spark.mesos.executor.docker.image') is None
    opts = spark(options_proxy, docker='testimage')()
    assert opts.get('spark.mesos.executor.docker.image') == 'testimage'


def test_executor_envs(options_proxy):
    envs = {'TEST_VARIABLE': 'test value',
            'TEST_ENV_VAR': 'test env value'}
    opts = spark(options_proxy, envs=envs)()
    for k, v in envs.items():
        assert opts.get('spark.executorEnv.{}'.format(k)) == v


def test_custom_options(options_proxy):
    custom_opts = {'spark.driver.maxResultSize': '2g',
                   'spark.shuffle.manager': 'hash',
                   'spark.shuffle.compress': False}
    opts = spark(options_proxy, options=custom_opts)()
    for k, v in custom_opts.items():
        assert opts.get(k) == str(v)


def test_decorated_job(decorated_sum):
    lst = range(100)
    assert decorated_sum(lst) == sum(lst)


def test_curried_job(curried_sum):
    lst = range(100)
    assert curried_sum(lst) == sum(lst)


# def test_mesos_master():
#     def job(sc, sql, lst):
#         rdd = sc.parallelize(lst)
#         return rdd.sum()

#     fn = spark(job, master='mesos://localhost:5050')
#     lst = range(100)

#     assert fn(lst) == sum(lst)

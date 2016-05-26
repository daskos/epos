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
    assert opts.get('spark.mesos.coarse') == 'False'

    opts = spark(options_proxy, coarse=10)()
    assert opts.get('spark.mesos.coarse') == 'True'
    assert opts.get('spark.cores.max') == '10'


def test_docker_executor(options_proxy):
    opts = spark(options_proxy)()
    assert opts.get('spark.mesos.executor.docker.image') == 'lensa/epos'
    opts = spark(options_proxy, docker='testimage')()
    assert opts.get('spark.mesos.executor.docker.image') == 'testimage'


def test_executor_envs(options_proxy):
    envs = {'TEST_VARIABLE': 'test value',
            'TEST_ENV_VAR': 'test env value'}
    opts = spark(options_proxy, envs=envs)()
    for k, v in envs.items():
        assert opts.get('spark.executorEnv.{}'.format(k)) == v


def test_custom_options(options_proxy):
    custom_opts = {'driver_maxResultSize': '2g',
                   'shuffle_compress': False}
    opts = spark(options_proxy, **custom_opts)()
    assert opts['spark.driver.maxResultSize'] == '2g'
    assert opts['spark.shuffle.compress'] == 'False'


def test_decorated_job(decorated_sum):
    lst = range(100)
    assert decorated_sum(lst) == sum(lst)


def test_curried_job(curried_sum):
    lst = range(100)
    assert curried_sum(lst) == sum(lst)


def test_local_master():
    @spark(master='local[1]', driver_memory=512, executor_memory=512,
           python_worker_memory=256)
    def job(sc, sql, lst):
        rdd = sc.parallelize(lst)
        return rdd.sum()

    lst = range(100)

    assert job(lst) == sum(lst)


def test_mesos_master():
    @spark(master='mesos://localhost:5050', docker='lensa/epos:dev',
           driver_memory=512, executor_memory=512,
           python_worker_memory=256,
           coarse=1, executor_cores=1)
    def job(sc, sql, lst):
        rdd = sc.parallelize(lst)
        return rdd.sum()

    lst = range(100)

    assert job(lst) == sum(lst)

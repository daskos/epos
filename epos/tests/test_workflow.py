import pandas as pd
from dask_mesos import get
from epos import mesos, spark
from epos.utils import MiB, GiB


@mesos
def generate():
    return [['a', 1, 120],
            ['b', 2, 220],
            ['c', 1, 320],
            ['d', 3, 420],
            ['e', 3, 520]]


@mesos(docker='lensa/epos:dev', cpus=0.1, mem=128 * MiB)
def summarize(data):
    df = pd.DataFrame(data, columns=['char', 'i', 'v'])
    summary = df.groupby('i').sum()
    return summary.to_dict(orient='records')


@mesos(docker='lensa/epos:dev', cpus=0.1, mem=1 * GiB)
@spark(master='mesos://localhost:5050', docker='lensa/epos:dev',
       driver_memory=512 * MiB, executor_memory=512 * MiB,
       python_worker_memory=256 * MiB,
       coarse=1, executor_cores=1)
def spark_maximum(sc, sqlctx, dcts):
    rdd = sc.parallelize(dcts, 2)
    return rdd.map(lambda x: x['v']).max()


@mesos(docker='lensa/epos:dev', cpus=0.1, mem=128 * MiB)
def maximum(dcts):
    prices = map(lambda x: x['v'], dcts)
    return max(prices)


@mesos(docker='lensa/epos:dev', cpus=0.1, mem=128 * MiB)
def aggregate(first, second):
    return first + second


@mesos(docker='lensa/epos:dev', cpus=0.1, mem=128 * MiB)
def add(a, b):
    return a + b


def workflow():
    data = generate()
    summary = summarize(data)
    max1 = maximum(summary)
    max2 = spark_maximum(summary)
    return add(max1, max2)


def test_result():
    result = workflow().compute()
    assert result == 1880


def test_mesos_result():
    result = workflow().compute(get=get)
    assert result == 1880

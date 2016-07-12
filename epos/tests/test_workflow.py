from __future__ import absolute_import, division, print_function

import os
import pytest
import pandas as pd
from epos import mesos, spark
from epos.utils import MiB, GiB


master = os.environ.get('MESOS_MASTER')
pytestmark = pytest.mark.skipif(
    not master, reason='MESOS_MASTER environment variable must be set')


@mesos
def generate():
    return [['a', 1, 120],
            ['b', 2, 220],
            ['c', 1, 320],
            ['d', 3, 420],
            ['e', 3, 520]]


@mesos(cpus=0.1, mem=128 * MiB)
def summarize(data):
    df = pd.DataFrame(data, columns=['char', 'i', 'v'])
    summary = df.groupby('i').sum()
    return summary.to_dict(orient='records')


@mesos(cpus=0.1, mem=1 * GiB)
@spark(master=master, docker='lensa/epos:latest',
       driver_memory=512 * MiB, executor_memory=512 * MiB,
       python_worker_memory=256 * MiB,
       coarse=1, executor_cores=1)
def spark_maximum(sc, sqlctx, dcts):
    rdd = sc.parallelize(dcts, 2)
    return rdd.map(lambda x: x['v']).max()


@mesos(cpus=0.1, mem=128 * MiB)
def maximum(dcts):
    prices = map(lambda x: x['v'], dcts)
    return max(prices)


@mesos(cpus=0.1, mem=128 * MiB)
def aggregate(first, second):
    return first + second


@mesos(cpus=0.1, mem=128 * MiB)
def add(a, b):
    return a + b


def workflow():
    data = generate()
    summary = summarize(data)
    max1 = maximum(summary)
    max2 = spark_maximum(summary)
    return add(max1, max2)


def test_result():
    g = workflow()
    result = g.compute()
    assert result == 1880

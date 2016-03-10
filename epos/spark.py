from copy import copy
from functools import wraps

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from toolz import curry

from .utils import envargs


@curry
@envargs(prefix='EPOS_SPARK_')
def spark(fn, name=None, coarse=False, docker=None, memory=None, role=None,
          files=[], pyfiles=[], options={}, envs={}, uris=[], constraints=[],
          log='ERROR'):
    """Decorator order matters! Spark always comes after/below mesos."""
    opts = copy(options)
    opts.update({
        # ensure that spark accepts 'True' instead of 'true'
        'spark.executor.memory': memory,
        'spark.coarse.max': coarse or None,
        'spark.mesos.role': role,
        'spark.mesos.coarse': bool(coarse),
        'spark.mesos.uris': ','.join(uris),
        'spark.mesos.executor.docker.image': docker,
        # TODO constraints
    })
    opts = [(k, str(v)) for k, v in opts.items() if v not in (None, '')]
    envs = envs.items()

    @wraps(fn)
    def wrapper(*args, **kwargs):
        conf = SparkConf()
        conf.setAppName(name or fn.__name__)
        conf.setAll(pairs=opts)
        conf.setExecutorEnv(pairs=envs)

        with SparkContext(conf=conf) as sc:
            sc.setLogLevel(log)
            map(sc.addFile, files)
            map(sc.addPyFile, pyfiles)
            sql = SQLContext(sc)
            return fn(sc, sql, *args, **kwargs)

    return wrapper

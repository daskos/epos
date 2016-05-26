from copy import copy
from functools import wraps


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from toolz import curry

from .utils import envargs, MiB, GiB


@curry
@envargs(prefix='EPOS_SPARK_')
def spark(fn, name=None, master='local[*]', docker='lensa/epos', role='*',
          envs={}, uris=[], files=[], pyfiles=[], driver_memory=4*GiB,
          coarse=False, executor_cores=4, executor_memory=4*GiB,
          memory_overhead=None, python_worker_memory=512*MiB,
          log='ERROR', **opts):
    """Decorator order matters!

    Spark always comes after/below mesos.
    """
    try:
        memory_overhead = memory_overhead or (
            executor_cores * python_worker_memory + 0.1 * executor_memory)

        opts.update({
            'sql_parquet_compression_codec': 'uncompressed',
            'mesos_role': role,
            'mesos_coarse': bool(coarse),
            'cores_max': coarse or None,
            'executor_cores': executor_cores,
            'executor_memory': '{}m'.format(int(executor_memory / MiB)),
            'driver_memory': '{}m'.format(int(driver_memory / MiB)),
            'mesos_executor_memoryOverhead': int(memory_overhead / MiB),
            'python_worker_memory': int(python_worker_memory / MiB),
            'spark.mesos.uris': ','.join(uris),
            'mesos_executor_docker_image': docker
        })
        opts = {'spark.{}'.format(k.replace('_', '.')): str(v)
                for k, v in opts.items() if v is not None}
        envs = envs.items()
    except TypeError as e:
        # curry doesn't reraise TypeErrors:
        # https://github.com/pytoolz/toolz/issues/288
        raise Exception(repr(e))

    @wraps(fn)
    def closure(*args, **kwargs):
        conf = SparkConf()
        conf.setMaster(master)
        conf.setAppName(name or fn.__name__)
        conf.setAll(pairs=opts.items())
        conf.setExecutorEnv(pairs=envs)

        with SparkContext(conf=conf) as sc:
            sc.setLogLevel(log)
            map(sc.addFile, files)
            map(sc.addPyFile, pyfiles)
            # TODO: user sparksession
            sql = SQLContext(sc)
            return fn(sc, sql, *args, **kwargs)

    return closure

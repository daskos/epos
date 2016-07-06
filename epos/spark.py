from __future__ import absolute_import, division, print_function

from toolz import curry
from functools import wraps
from dask import delayed

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from .utils import MiB, GiB
from .context import lazyargs


@curry
@lazyargs
def spark(fn, name=None, master='local[*]', docker='lensa/epos', role='*',
          envs={}, uris=[], files=[], pyfiles=[], driver_memory=4 * GiB,
          coarse=False, executor_cores=4, executor_memory=4 * GiB,
          memory_overhead=None, python_worker_memory=512 * MiB,
          log='ERROR', **opts):
    """Spark context decorator"""
    @wraps(fn, assigned=('__name__', '__doc__'))
    def closure(*args, **kwargs):
        try:
            options = opts
            options.update({
                'sql_parquet_compression_codec': 'uncompressed',
                'mesos_role': role,
                'mesos_coarse': bool(coarse),
                'cores_max': int(coarse) or None,
                'executor_cores': int(executor_cores),
                'executor_memory': '{}m'.format(int(executor_memory / MiB)),
                'driver_memory': '{}m'.format(int(driver_memory / MiB)),
                'mesos_executor_memoryOverhead': int(
                    (memory_overhead or (executor_cores * python_worker_memory +
                                         0.1 * executor_memory))
                    / MiB),
                'python_worker_memory': int(python_worker_memory / MiB),
                'mesos_uris': ','.join(uris),
                'mesos_executor_docker_image': docker
            })
            options = {'spark.{}'.format(k.replace('_', '.')): str(v)
                       for k, v in options.items() if v not in (None, '')}
            environs = envs.items()
        except TypeError as e:
            # curry doesn't reraise TypeErrors:
            # https://github.com/pytoolz/toolz/issues/288
            raise Exception(repr(e))

        conf = SparkConf()
        conf.setMaster(str(master))
        conf.setAppName(str(name or fn.__name__))
        conf.setAll(pairs=options.items())
        conf.setExecutorEnv(pairs=environs)

        with SparkContext(conf=conf) as sc:
            sc.setLogLevel(str(log))
            map(sc.addFile, files)
            map(sc.addPyFile, pyfiles)
            # TODO: user sparksession
            sql = SQLContext(sc)
            return fn(sc, sql, *args, **kwargs)

    return closure

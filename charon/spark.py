from functools import wraps
from toolz import curry

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

"""Decorator order matters! Spark always comes after/below mesos."""


@curry
def spark(fn, name=None, coarse=False, docker='default', memory=None,
          files=[], pyfiles=[], options={}, envs={}):
    options.update({
        #ensure that spark accepts 'True' instead of 'true'
        "spark.mesos.coarse": False,
        "spark.coarse.max": coarse or None,
        "spark.executor.memory": memory})
    options = [(k, str(v)) for k, v in options.items() if v]
    envs = envs.items()

    @wraps(fn)
    def wrapper(*args, **kwargs):
        conf = SparkConf()
        conf.setAppName(name or fn.__name__)
        conf.setAll(pairs=options)
        conf.setExecutorEnv(pairs=envs)

        with SparkContext(conf=conf) as sc:
            map(sc.addFile, files)
            map(sc.addPyFile, pyfiles)
            sql = SQLContext(sc)
            return fn(sc, sql, *args, **kwargs)

    return wrapper

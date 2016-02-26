from odo import HDFS, append
from odo.resource import resource
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext


class Parquet(object):
    canonical_extension = 'parquet'

    def __init__(self, path, **kwargs):
        self.path = path


@resource.register('.+\.(parquet)?')
def resource_parquet(uri, **kwargs):
    return Parquet(uri)


@append.register(Parquet, SparkDataFrame)
def sparksql_dataframe_to_parquet(p, df, dshape=None, **kwargs):
    df.write.parquet("file://" + p.path)
    return p


@append.register(SQLContext, Parquet)
def parquet_to_sparksql_dataframe(ctx, p, dshape=None, **kwargs):
    df = ctx.read.parquet(p.path)
    return df


@append.register(SQLContext, HDFS(Parquet))
def hdfs_parquet_to_sparksql_dataframe(p, ctx, dshape=None, **kwargs):
    df = ctx.read.parquet(p.path)
    return df


@append.register(HDFS(Parquet), SparkDataFrame)
def sparksql_dataframe_to_hdfs_parquet(p, df, dshape=None, **kwargs):
    df.write.parquet(p.path)
    return p

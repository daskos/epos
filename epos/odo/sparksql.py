from __future__ import absolute_import, division, print_function

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext

from odo import convert, append
from .parquet import Parquet


@append.register(Parquet, SparkDataFrame)
def sparksql_dataframe_to_parquet(p, df, dshape=None, **kwargs):
    return df.write.parquet("file://" + p.path)


@append.register(SQLContext, Parquet)
def parquet_to_sparksql_dataframe(ctx, p, dshape=None, **kwargs):
    return ctx.read.parquet("file://" + p.path)


try:
    from odo import HDFS
except ImportError:
    pass
else:
    @append.register(SQLContext, HDFS(Parquet))
    def hdfs_parquet_to_sparksql_dataframe(ctx, p, dshape=None, **kwargs):
        path = 'hdfs://{host}/{path}'.format(host=p.hdfs.host,
                                             path=p.path.lstrip('/'))
        return ctx.read.parquet(path)

    @append.register(HDFS(Parquet), SparkDataFrame)
    def sparksql_dataframe_to_hdfs_parquet(p, df, dshape=None, **kwargs):
        path = 'hdfs://{host}/{path}'.format(host=p.hdfs.host,
                                             path=p.path.lstrip('/'))
        return df.write.parquet(path)

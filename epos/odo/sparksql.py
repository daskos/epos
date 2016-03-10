from __future__ import absolute_import, division, print_function

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext

from odo import append
from .parquet import Parquet
from .cassandra import Cassandra


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
        path = 'hdfs://{user}@{host}/{path}'.format(user=p.hdfs.user_name,
                                                    host=p.hdfs.host,
                                                    path=p.path.lstrip('/'))
        return ctx.read.parquet(path)

    @append.register(HDFS(Parquet), SparkDataFrame)
    def sparksql_dataframe_to_hdfs_parquet(p, df, dshape=None, **kwargs):
        path = 'hdfs://{user}@{host}/{path}'.format(user=p.hdfs.user_name,
                                                    host=p.hdfs.host,
                                                    path=p.path.lstrip('/'))
        return df.write.parquet(path)


@append.register(Cassandra, SparkDataFrame)
def sparksql_dataframe_to_cassandra(c, df, dshape=None, **kwargs):
    (df.write
     .format("org.apache.spark.sql.cassandra")
     .options(table=c.table, keyspace=c.keyspace)
     .save(mode="append"))
    return c


@append.register(SQLContext, Cassandra)
def cassandra_to_sparksql_dataframe(ctx, c, dshape=None, **kwargs):
    return (ctx.read
            .format("org.apache.spark.sql.cassandra")
            .options(table=c.table, keyspace=c.keyspace)
            .load())

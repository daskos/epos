import re

from odo import append, resource


class Cassandra(object):

    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host', None)
        self.keyspace = kwargs.get('keyspace', None)
        self.table = kwargs.get('table', None)


try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SQLContext
except ImportError:
    pass
else:
    @resource.register(r'cql://.+')
    def resource_cassandra(uri, **kwargs):
        c = re.findall(r'cql:\/\/(.*)::(.*)', uri)
        return Cassandra(host=None, keyspace=c[0][0], table=c[0][1])

    @append.register(Cassandra, SparkDataFrame)
    def sparksql_dataframe_to_cassandra(c, df, dshape=None, **kwargs):
        (df.write
         .format("org.apache.spark.sql.cassandra")
         .options(table=c.table, keyspace=c.keyspace)
         .save(mode="append"))
        return c

    @append.register(SQLContext, Cassandra)
    def cassandra_to_sparksql_dataframe(ctx, c, dshape=None, **kwargs):
        df = (ctx.read
              .format("org.apache.spark.sql.cassandra")
              .options(table=c.table, keyspace=c.keyspace)
              .load())
        return df

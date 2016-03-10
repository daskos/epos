import re

from odo import append, resource
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext
# from cassandra.cluster import Cluster


class Cassandra(object):
    setup_regex = r'cql:\/\/(?P<host>[a-z0-9-\.]*)(:(?P<port>[0-9]*))?\/(?P<keyspace>[a-zA-Z][a-zA-Z0-9_]*)'

    def __init__(self, host='localhost', port='9042', keyspace='', table=''):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.table = table
        # self.cass = self.create_cassandra_session(host, port)

    # def create_cassandra_session(self, host, port):
        # c = Cluster(host.split(','), port=port)
        # return c.connect()


@resource.register(r'cql://.+')
def resource_cassandra(uri, *args, **kwargs):
    m = re.match(Cassandra.setup_regex, uri)
    return Cassandra(table=args[0], **{k: v for k, v in m.groupdict().items() if v})

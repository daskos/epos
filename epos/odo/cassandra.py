import re

from odo import resource


class Cassandra(object):
    setup_regex = ('cql:\/\/(?P<host>[a-z0-9-\.]*)(:(?P<port>[0-9]*))?\/'
                   '(?P<keyspace>[a-zA-Z][a-zA-Z0-9_]*)')

    def __init__(self, host='localhost', port='9042', keyspace='', table=''):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.table = table


@resource.register(r'cql://.+')
def resource_cassandra(uri, *args, **kwargs):
    m = re.match(Cassandra.setup_regex, uri)
    params = {k: v for k, v in m.groupdict().items() if v}
    return Cassandra(table=args[0], **params)

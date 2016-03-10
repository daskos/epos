from __future__ import absolute_import, division, print_function

from odo import resource
from epos.odo.cassandra import Cassandra


def test_resource():
    res = resource('cql://test-host:9042/keyspace::table')
    assert isinstance(res, Cassandra)
    assert res.host == 'test-host'
    assert res.port == '9042'
    assert res.keyspace == 'keyspace'
    assert res.table == 'table'


def test_resource_without_port():
    """It should fill it in w/ the default value."""
    res = resource('cql://test-host/keyspace::table')
    assert isinstance(res, Cassandra)
    assert res.host == 'test-host'
    assert res.port == '9042'
    assert res.keyspace == 'keyspace'
    assert res.table == 'table'

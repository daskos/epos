from __future__ import absolute_import, division, print_function

import pytest
from odo import resource
from epos.odo.cassandra import Cassandra


def test_resource():
    res = resource('cql://bdas-worker-4:9042/github::readmes')
    assert isinstance(res, Cassandra)
    assert res.host == 'bdas-worker-4'
    assert res.port == '9042'
    assert res.keyspace == 'github'
    assert res.table == 'readmes'


def test_resource_without_port():
    """It should fill it in w/ the default value."""
    res = resource('cql://bdas-worker-4/github::readmes')
    assert isinstance(res, Cassandra)
    assert res.host == 'bdas-worker-4'
    assert res.port == '9042'
    assert res.keyspace == 'github'
    assert res.table == 'readmes'

from __future__ import absolute_import, division, print_function

import os
import pytest
import operator

from epos import mesos
from epos.context import set_options, _globals


master = os.environ.get('MESOS_MASTER')
pytestmark = pytest.mark.skipif(
    not master, reason='MESOS_MASTER environment variable must be set')


@mesos
def proxy(key):
    return _globals[key]


@mesos(cpus=0.2)
def proxy1(key):
    return _globals[key]


def test_without_arguments():
    add = mesos(operator.add)
    mul = mesos(operator.mul)

    s = add(1, 2)
    m = mul(s, 3)

    assert m.compute() == 9


def test_forwarding_context():
    with set_options(testkey={'test': 'val'},
                     mesos={'cpus': 15}):
        value = proxy('testkey')
        assert value.compute() == {'test': 'val'}

    with set_options(testkey1={'test1': 'val'}):
        value = proxy1('testkey1')
        assert value.compute() == {'test1': 'val'}

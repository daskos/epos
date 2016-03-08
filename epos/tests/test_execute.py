from __future__ import absolute_import, division, print_function

import pytest
from epos.execute import loads, dumps, run, bash
from subprocess import Popen, PIPE
from operator import add


def test_dumps_string():
    serialized = dumps(add, args=[1, 2])
    assert isinstance(serialized, str)

    serialized = dumps(add, kwargs={'a': 1, 'b': 2})
    assert isinstance(serialized, str)


def test_loads_triple():
    serialized = dumps(add, args=[1, 2])
    fn, args, kwargs = loads(serialized)
    assert fn == add
    assert args == [1, 2]
    assert kwargs == {}

    serialized = dumps(add, kwargs={'a': 1, 'b': 2})
    fn, args, kwargs = loads(serialized)
    assert fn == add
    assert args == []
    assert kwargs == {'a': 1, 'b': 2}


def test_execution():
    serialized = dumps(add, args=[1, 2])
    assert run(serialized) == add(1, 2)


def test_bash_execution():
    def print_add(a, b):
        print(a + b)

    callback = dumps(print_add, args=[1, 2])
    env = {'PYTHONPATH': '.:.eggs/cloudpickle-0.2.1-py2.7.egg:'}
    cmd = ['python', '-c', bash, callback]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, env=env)
    stdout, stderr = p.communicate()
    assert stdout.strip() == str(3)

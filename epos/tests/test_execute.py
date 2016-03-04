import pytest
from epos.execute import loads, dumps, run
import subprocess
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
    callback = dumps(add, args=[1, 2])
    cmd = 'python -m epos.execute {}'.format(callback)
    print subprocess.call(cmd)
    print cmd

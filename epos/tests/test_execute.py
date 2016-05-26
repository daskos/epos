from __future__ import absolute_import, division, print_function

from epos.execute import loads, dumps, run, command
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
    def print_add(a, b):
        print(a + b)

    cmd = command(print_add, args=[1, 2], kwargs={},
                  path='.eggs/cloudpickle-0.2.1-py2.7.egg')

    stdout = subprocess.check_output(cmd, shell=True)
    assert stdout.strip() == str(3)

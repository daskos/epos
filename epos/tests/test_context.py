from operator import add, mul

import numpy as np
import pytest
from cloudpickle import loads
from dask import get
from dask.callbacks import Callback
from epos.context import Lock, Persist
from kazoo.client import NoNodeError
from kazoo.recipe.lock import LockTimeout
from numpy.testing import assert_array_equal


class Ran(Callback):

    def __init__(self, *args, **kwargs):
        self.steps = []

    def _pretask(self, key, dsk, state):
        self.steps.append(key)


@pytest.fixture(scope="module")
def dsk1():
    return {'x': 1,
            'y': 2,
            'z': (add, 'x', 'y'),
            'w': (sum, ['x', 'y', 'z'])}


@pytest.fixture(scope="module")
def dsk2():
    return {'a': np.arange(10),
            'b': 5,
            'e': (mul, 'a', 'b'),
            'f': (lambda a, power: a ** power, 'a', 2),
            's': (lambda x, y: x / y, 'f', 'e'),
            'w': (add, 'f', 's')}


def test_persisting(zk, dsk1):
    with Persist(zk, name="dsk1"):
        with Ran() as r:
            assert get(dsk1, 'w') == 6
            assert r.steps == ['z', 'w']
        with Ran() as r:
            assert get(dsk1, 'w') == 6
            assert r.steps == []

        assert loads(zk.get("/epos/dsk1/z")[0]) == 3
        assert loads(zk.get("/epos/dsk1/w")[0]) == 6

    # tests ephemeral=False, znode still exists after context handler
    assert loads(zk.get("/epos/dsk1/w")[0]) == 6


def test_ephemeral_persisting(zk, dsk2):
    with Persist(zk, name="dsk2", ns="/test/dags", ephemeral=True):
        with Ran() as r:
            assert_array_equal(get(dsk2, 'e'), np.arange(10) * 5)
            assert r.steps == ['e']
        with Ran() as r:
            assert_array_equal(get(dsk2, 's'),
                               np.arange(10) ** 2 / (np.arange(10) * 5))
            assert r.steps == ['f', 's']
        with Ran() as r:
            assert_array_equal(get(dsk2, 's'),
                               np.arange(10) ** 2 / (np.arange(10) * 5))
            assert r.steps == []

        assert_array_equal(loads(zk.get("/test/dags/dsk2/e")[0]),
                           np.arange(10) * 5)

    with pytest.raises(NoNodeError):
        zk.get("/test/dags/dsk2/e")


def test_locking(zk, dsk1):
    with pytest.raises(LockTimeout):
        # two identical locks; the second cannot acquire
        with Lock(zk, name="dsk1", timeout=1), \
                Lock(zk, name="dsk1", timeout=1):
            get(dsk1, 'w')

    # test lock release
    with Lock(zk, name="dsk1"):
        get(dsk1, 'w')
        get(dsk1, 'w')


def test_ephemeral_locking(zk, dsk2):
    with pytest.raises(LockTimeout):
        with Lock(zk, name="dsk2", timeout=1, ephemeral=True), \
                Lock(zk, name="dsk2", timeout=1, ephemeral=True):
            get(dsk2, 'f')

    with pytest.raises(NoNodeError):
        zk.get("/epos/dsk2")

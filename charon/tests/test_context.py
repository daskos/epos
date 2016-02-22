import pytest

from kazoo.testing import KazooTestHarness
from kazoo.recipe.lock import LockTimeout
from charon.context import TaskLock
from dask import get
from time import sleep
from operator import add

def add(a, b):
    sleep(0.5)
    return a + b


class KazooTest(KazooTestHarness):

    def __init__(self):
        self.client = None
        self._clients = []


@pytest.fixture(scope="module")
def zk(request):
    zk = KazooTest()
    zk.setup_zookeeper()
    request.addfinalizer(zk.teardown_zookeeper)
    return zk.client


@pytest.fixture(scope="module")
def dsk():
    return {'x': 1,
            'y': 2,
            'z': (add, 'x', 'y'),
            'w': (sum, ['x', 'y', 'z'])}


def test_task_lock(dsk, zk):
    with pytest.raises(LockTimeout):
        with TaskLock(zk, timeout=0.01) as tl1, \
             TaskLock(zk, timeout=0.02) as tl2:
             get(dsk, 'w')

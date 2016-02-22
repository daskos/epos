from operator import add

import pytest
from charon.context import TaskLock
from dask import get
from kazoo.recipe.lock import LockTimeout
from kazoo.testing import KazooTestHarness


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


def test_cleanup(dsk, zk):
    with TaskLock(zk, timeout=1) as tl1:
        get(dsk, 'w')
    with TaskLock(zk, template='/test/{key}', timeout=1) as tl2:
        get(dsk, 'w')

    assert tl1.template == '/charon/task/{key}/lock'
    assert tl2.template == '/test/{key}'
    assert zk.exists('/charon/task/w/lock').data_length == 0
    assert zk.exists('/test/w').data_length == 0


def test_lock_raises(dsk, zk):
    with TaskLock(zk):
        assert get(dsk, 'w') == 6

    with pytest.raises(LockTimeout):
        with TaskLock(zk, timeout=0.01), TaskLock(zk, timeout=0.01):
            get(dsk, 'w')

    assert zk.exists('/charon/task/w/lock').data_length == 0


def test_release_lock(dsk, zk):
    with TaskLock(zk, timeout=0.1):
        get(dsk, 'z')
        get(dsk, 'w')
        get(dsk, 'w')

    assert zk.exists('/charon/task/w/lock').data_length == 0


def test_lock_custom_path(dsk, zk):
    with TaskLock(zk, template='/workflow/test/{key}', timeout=0.01), \
            TaskLock(zk, template='/staging/{key}/lock', timeout=0.01):
        get(dsk, 'w')

    assert zk.exists('/workflow/test/w').data_length == 0
    assert zk.exists('/staging/w/lock').data_length == 0

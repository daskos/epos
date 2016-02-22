import json

import multiprocessing

from dask.callbacks import Callback
from kazoo.recipe.lock import Lock


class Persist(Callback):

    def __init__(self, zk, name):
        self.zk = zk
        self.path = '/dagos/states/{}'.format(name)

    def _start(self, dsk):
        self.zk.ensure_path(self.path)
        states = json.loads(self.zk.get(self.path))

        overlap = set(dsk) & set(states)
        for key in overlap:
            dsk[key] = states[key]

    def _posttask(self, key, value, dsk, state, id):
        self.zk.set(self.path,
                    json.dumps(state, ensure_ascii=True))


class TaskLock(Callback):

    def __init__(self, zk, ns='global', path='/charon/locks/{ns}/{task}',
                 blocking=True, timeout=None, *args, **kwargs):
        self.zk = zk
        self.ns = ns
        self.path = path
        self.blocking = blocking
        self.timeout = timeout
        self.locks = {}

    def _start(self, dsk):
        path = self.path.format(ns=self.ns, task='').rstrip('/')
        self.zk.ensure_path(path)

    def _pretask(self, key, dsk, state):
        path = self.path.format(ns=self.ns, task=key)
        self.locks[key] = Lock(self.zk, path)
        self.locks[key].acquire(blocking=self.blocking,
                                timeout=self.timeout)

    def _posttask(self, key, value, dsk, state, id):
        self.locks[key].release()
        del self.locks[key]

    def _finish(self, dsk, state, failed):
        for key, lock in enumerate(self.locks):
            lock.release()
            del self.locks[key]

        path = self.path.format(ns=self.ns, task='').rstrip('/')
        self.zk.delete(path, recursive=True)

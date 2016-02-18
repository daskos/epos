import json

from dask.callbacks import Callback


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

    def __init__(self, zk, blocking=True, timeout=None,
                 *args, **kwargs):
        self.zk = zk
        self.locks = {}
        self.blocking = blocking
        self.timeout = timeout

    def _pretask(self, key, dsk, state):
        self.locks[key] = self.zk.Lock("/dagos/locks/{}".format(key))
        self.locks[key].acquire(blocking=self.blocking,
                                timeout=self.timeout)

    def _posttask(self, key, value, dsk, state, id):
        self.locks[key].release(key)
        del self.locks[key]

import cloudpickle
from dask.callbacks import Callback
from kazoo.recipe.lock import Lock as ZkLock


class ZookeeperBase(Callback):

    def __init__(self, zk, template):
        self.zk = zk
        self.template = template

    def path(self, *args, **kwargs):
        return self.template.format(*args, **kwargs).rstrip('/')

    def _cleanup(self):
        pass

    def _load(self, key):
        return cloudpickle.loads(self.zk.get(self.path(task=key)))

    def _save(self, key, value):
        return self.zk.create(self.path(key=key), cloudpickle.dumps(value),
                              makepath=True)

    def _finish(self, dsk, state, failed):
        self._cleanup()

    def __exit__(self, *args):
        self._cleanup()
        super(ZookeeperBase, self).__exit__(*args)


class Persist(ZookeeperBase):

    def __init__(self, zk, name, template='/charon/dask/{key}/state'):
        super(Persist, self).__init__(zk=zk, template=template)
        self.name = name

    def _start(self, dsk, state):
        persisted = self._load(self.name)
        state.update(persisted)

    def _posttask(self, key, value, dsk, state, id):
        self._save(self.name, state)


class Lock(ZookeeperBase):

    def __init__(self, zk, name, template='/charon/dask/{key}/lock',
                 blocking=None, timeout=None):
        super(Lock, self).__init__(zk=zk, template=template)
        self.name = name
        self.blockin = blocking
        self.timeout = timeout

    def _cleanup(self):
        self.lock.release()

    def _start(self, dsk, state):
        self.lock = ZkLock(self.zk, self.path(key=self.name))
        self.lock.acquire(blocking=self.blocking,
                          timeout=self.timeout)


class TaskPersist(ZookeeperBase):

    def __init__(self, zk, template='/charon/task/{key}/state'):
        super(TaskPersist, self).__init__(self, zk=zk, template=template)

    def _start(self, dsk, state):
        persisted = {k: self._load(k) for k, v in dsk.items()}
        overlap = set(dsk) & set(persisted)
        for key in overlap:
            dsk[key] = persisted[key]

    def _posttask(self, key, value, dsk, state, id):
        self._save(key, value)


class TaskLock(ZookeeperBase):

    def __init__(self, zk, template='/charon/task/{key}/lock',
                 blocking=True, timeout=None):
        super(TaskLock, self).__init__(zk=zk, template=template)
        self.blocking = blocking
        self.timeout = timeout
        self.locks = {}

    def _cleanup(self):
        for key, lock in self.locks.items():
            lock.release()
            del self.locks[key]

    def _pretask(self, key, dsk, state):
        self.locks[key] = ZkLock(self.zk, self.path(key=key))
        self.locks[key].acquire(blocking=self.blocking,
                                timeout=self.timeout)

    def _posttask(self, key, value, dsk, state, id):
        self.locks[key].release()
        del self.locks[key]

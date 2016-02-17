from dask.imperative import do
from dask import set_options
from toolz import curry


@curry
def mesos(fn, cpu=1, mem=128, disk=None, docker=None,
          contraints=None, lock=None, pure=True):
    wrapper = do(fn, pure=pure)
    wrapper.contraints = contraints
    wrapper.resources = {'cpu': cpu,
                         'mem': mem,
                         'disk': disk}
    wrapper.docker = docker
    wrapper.lock = lock
    return wrapper

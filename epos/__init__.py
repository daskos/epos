from __future__ import absolute_import, division, print_function

from contextlib import contextmanager

@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass

with ignoring(ImportError):
    from dask_mesos import mesos

with ignoring(ImportError):
    from .context import Lock, Persist

with ignoring(ImportError):
    from .spark import spark

from dask.imperative import do
from .chronos import chronos
from .marathon import marathon

from __future__ import absolute_import, division, print_function

import logging
import pkg_resources as _pkg_resources
from contextlib import contextmanager

logging.basicConfig(format='%(relativeCreated)6d %(threadName)s %(message)s')

__version__ = _pkg_resources.get_distribution('epos').version


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


from dask import delayed
from functools import partial

with ignoring(ImportError):
    from .mesos import mesos

with ignoring(ImportError):
    from .context import Lock, Persist

with ignoring(ImportError):
    from .spark import spark


from .odo import odo
from .chronos import chronos
from .marathon import marathon

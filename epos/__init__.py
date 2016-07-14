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


from .odo import odo
from .context import set_options, options, lazyargs, _globals
from .chronos import chronos
from .marathon import marathon

with ignoring(ImportError):
    from .mesos import mesos

with ignoring(ImportError):
    from .spark import spark

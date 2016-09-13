from __future__ import absolute_import, division, print_function

import pkg_resources as _pkg_resources
from contextlib import contextmanager


__version__ = _pkg_resources.get_distribution('epos').version


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


from .context import set_options, options, lazyargs, _globals
from .chronos import chronos
from .marathon import marathon

with ignoring(ImportError):
    from .odo import odo

with ignoring(ImportError):
    from .mesos import mesos

with ignoring(ImportError):
    from .spark import spark

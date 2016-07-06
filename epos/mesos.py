from __future__ import absolute_import, division, print_function

from dask_mesos import mesos as mesos_delayed

from dask import delayed
from toolz import curry

from .context import set_options, lazyargs


set_options(mesos=dict(docker='lensa/epos'))
mesos = lazyargs(mesos_delayed)

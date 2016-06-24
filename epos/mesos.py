from __future__ import absolute_import, division, print_function

from dask_mesos import mesos as mesos_delayed

from .context import options, set_options


set_options(mesos=dict(docker='lensa/epos'))
mesos = options(mesos_delayed)

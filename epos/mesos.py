from __future__ import absolute_import, division, print_function

from dask_mesos import mesos as mesos_delayed

from .context import envargs


mesos = envargs(mesos_delayed, prefix='MESOS')

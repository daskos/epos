from __future__ import absolute_import, division, print_function

import click
from copy import copy

from ..context import _globals
from ..utils import GiB, MiB


CONTEXT_SETTINGS = dict(auto_envvar_prefix='EPOS')


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--cpus', type=float,
              help='Default amount of cpus allocated for mesos jobs')
@click.option('--mem',
              help=('Default amount of memory allocated for mesos jobs; '
                    'formats 128m or 1.2g'))
@click.option('--docker',
              help=('Default docker images used by mesos, marathon and chronos'
                    'jobs'))
@click.option('--uris', '-u', multiple=True,
              help='Mesos uris to be fetched by executors')
@click.option('--mesos-master', default='localhost:5050',
              help=('Mesos master host e.g. '
                    'zk://master1.host:2181,master2,host:2181/mesos'))
@click.option('--marathon-host', default='localhost:8080',
              help='Marathon API host with port in format marathon.host:8080')
@click.option('--chronos-host', default='localhost:4400',
              help='Chronos API host with port in format chronos.host:4400')
def epos(cpus, mem, docker, uris, mesos_master, marathon_host, chronos_host):
    """Epos

    DAG Workflows like epic poems via Mesos, Marathon, Chronos, Spark, Dask
    """
    units = {'m': MiB, 'g': GiB}

    commons = {'cpus': cpus,
               'mem': int(float(mem[:-1]) * units[mem[-1]]) if mem else None,
               'docker': docker,
               'uris': uris}
    commons = {k: v for k, v in commons.items() if v}

    for key in ('mesos', 'marathon', 'chronos'):
        _globals[key] = copy(commons)

    _globals['mesos']['master'] = mesos_master
    _globals['chronos']['host'] = chronos_host
    _globals['marathon']['host'] = marathon_host

from __future__ import absolute_import, division, print_function

import click

from ..context import _globals
from ..utils import GiB, MiB


CONTEXT_SETTINGS = dict(auto_envvar_prefix='EPOS')


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--cpus', type=float, default=1,
              help='Default amount of cpus allocated for mesos jobs')
@click.option('--mem', default='128m',
              help=('Default amount of memory allocated for mesos jobs; '
                    'formats 128m or 1.2g'))
@click.option('--docker', default='lensa/epos',
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
def epos(cpus, mem, docker, uris, **kwargs):
    """Epos

    DAG Workflows like epic poems via Mesos, Marathon, Chronos, Spark, Dask"""

    units = {'m': MiB, 'g': GiB}
    mem = int(float(mem[:-1]) * units[mem[-1]])

    commons = {'cpus': cpus,
               'mem': mem,
               'docker': docker,
               'uris': uris}
    for key, value in commons.items():
        for prefix in ('mesos', 'marathon', 'chronos'):
            kwargs['_'.join([prefix, key]).upper()] = value

    _globals['envs'] = kwargs  # used by envargs decorators

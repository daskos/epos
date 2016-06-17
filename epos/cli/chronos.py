from __future__ import absolute_import, division, print_function

import click
import pandas as pd
from tabulate import tabulate

from ..chronos import jobs
from ..utils import GiB


def docker_image(x):
    try:
        return x['image']
    except:
        return ''


@click.group()
@click.version_option()
def chronos():
    """Chronos CLI"""


@chronos.command('jobs')
@click.option('--host', default='localhost:4400')
@click.option('--sort', default='name')
def applications(host, sort):
    """List Chronos applications"""
    df = pd.DataFrame(jobs(host=host))
    df = df[['name', 'cpus', 'mem', 'schedule', 'container']]
    df['container'] = df.container.apply(docker_image)
    df['mem'] = df.mem / GiB
    df.sort_values(sort, inplace=True)
    click.echo(tabulate(df.values, headers=df.columns.tolist(),
                        tablefmt='psql', floatfmt=".1f"))

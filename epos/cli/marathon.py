from __future__ import absolute_import, division, print_function

import click
import pandas as pd
from tabulate import tabulate

from ..marathon import apps, tasks
from ..utils import GiB


def docker_image(x):
    try:
        return x['docker']['image']
    except:
        return ''


@click.group()
@click.version_option()
def marathon():
    """Marathon CLI"""


@marathon.command('apps')
@click.option('--host', default='localhost:8080')
@click.option('--sort', default='id')
def applications(host, sort):
    """List Marathon applications"""
    df = pd.DataFrame(apps(host=host))
    df = df[['id', 'cpus', 'mem', 'instances', 'container']]
    df['container'] = df.container.apply(docker_image)
    df['mem'] = df.mem / GiB
    df.sort_values(sort, inplace=True)
    click.echo(tabulate(df.values, headers=df.columns.tolist(),
                        tablefmt='psql', floatfmt=".1f"))


@marathon.command('tasks')
@click.argument('app', nargs=1)
@click.option('--host', default='localhost:8080')
@click.option('--sort', default='id')
def application_tasks(app, host, sort):
    """List tasks belonging to an application"""
    df = pd.DataFrame(tasks(host=host, app=app))
    df = df[['id', 'host', 'stagedAt', 'startedAt']]
    df.sort_values(sort, inplace=True)
    click.echo(tabulate(df.values, headers=df.columns.tolist(),
                        tablefmt='psql', floatfmt=".1f"))

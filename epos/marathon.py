from __future__ import absolute_import, division, print_function

import requests

from operator import itemgetter
from six import wraps
from toolz import curry
from dask.delayed import tokenize

from .utils import http_endpoint
from .execute import command
from .context import lazyargs


start = http_endpoint(resource='/v2/apps', method=requests.post)
destroy = http_endpoint(resource='/v2/apps/{id}', method=requests.delete)
restart = http_endpoint(resource='/v2/apps/{id}/restart', method=requests.post)

apps = http_endpoint(resource='/v2/apps', extract=itemgetter('apps'))
tasks = http_endpoint(resource='/v2/apps/{app}/tasks',
                      extract=itemgetter('tasks'))
deployments = http_endpoint(resource='/v2/deployments')

app = http_endpoint(resource='/v2/apps/{id}', extract=itemgetter('app'))
task = http_endpoint(resource='/v2/apps/{app}/tasks/{id}')
deployment = http_endpoint(resource='/v2/deployments/{id}')


def _parse_volumes(vols):
    vols = [(v + ':rw').split(':')[:3] for v in vols]
    return [{'hostPath': host,
             'containerPath': container,
             'mode': mode.upper()}
            for host, container, mode in vols]


@curry
@lazyargs
def marathon(fn, name=None, cpus=0.1, mem=128, instances=1,
             docker='lensa/epos', force_pull=False, envs={}, uris=[],
             volumes=[], path='$PYTHONPATH', host='localhost:8080'):
    """Marathon job launcher"""
    @wraps(fn, assigned=('__name__', '__doc__'))
    def wrapper(*args, **kwargs):
        payload = {
            'cpus': float(cpus),
            'mem': float(mem),
            'instances': int(instances),
            'env': dict(envs),
            'uris': list(uris)
        }
        if docker:
            payload['container'] = {'docker': {
                'image': str(docker),
                'forcePullImage': bool(force_pull)}
            }
            if volumes:
                payload['container']['volumes'] = _parse_volumes(volumes)

        mid = '{}-{}'.format(name or fn.__name__, tokenize(*args, **kwargs))
        payload['id'] = mid
        payload['cmd'] = command(fn, args, kwargs, path=path)
        start(host=host, payload=payload)
        return mid

    return wrapper

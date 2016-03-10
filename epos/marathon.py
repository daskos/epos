from __future__ import absolute_import, division, print_function

import os
import requests

from functools import wraps
from toolz import curry

from .utils import http_endpoint, envargs
from .execute import command


default_host = os.environ.get('MARATHON_HOST')
endpoint = http_endpoint(host='{host}/v2'.format(host=default_host))

start = endpoint(resource='/apps', method=requests.post)
destroy = endpoint(resource='/apps/{id}', method=requests.delete)
restart = endpoint(resource='/apps/{id}/restart', method=requests.post)

apps = endpoint(resource='/apps')
tasks = endpoint(resource='/apps/{app}/tasks')
deployments = endpoint(resource='/deployments')

app = endpoint(resource='/apps/{id}')
task = endpoint(resource='/apps/{app}/tasks/{id}')
deployment = endpoint(resource='/deployments/{id}')


def _parse_volumes(vols):
    vols = [(v + ':rw').split(':')[:3] for v in vols]
    return [{'hostPath': host,
             'containerPath': container,
             'mode': mode.upper()}
            for host, container, mode in vols]


@curry
@envargs(prefix='EPOS_MARATHON_')
def marathon(fn, name=None, cpus=0.1, mem=128, instances=1, image='python',
             path='$PYTHONPATH', envs={}, uris=[], volumes=[],
             host=default_host):
    payload = {
        'id': name or fn.__name__,
        'cpus': float(cpus),
        'mem': float(mem),
        'instances': int(instances),
        'env': dict(envs),
        'uris': list(uris)
    }
    if image:
        payload['container'] = {'docker': {'image': str(image)}}
        if volumes:
            payload['container']['volumes'] = _parse_volumes(volumes)

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload['cmd'] = command(fn, args, kwargs, path=path)
        return start(payload=payload)

    return wrapper

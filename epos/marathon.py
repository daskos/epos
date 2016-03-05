from __future__ import absolute_import, division, print_function

from copy import copy
from functools import wraps

import os
import requests
from toolz import curry
from .execute import loads, dumps
from .utils import http_endpoint


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


@curry
def marathon(fn, name=None, cpus=1, mem=512, instances=1, image='python',
             envs={}, host=default_host):
    payload = {
        'id': name or fn.__name__,
        'cpus': float(cpus),
        'mem': float(mem),
        'instances': int(instances),
        'env': dict(envs)
    }
    if image:
        payload['container'] = {'docker': {'image': str(image)}}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        callback = dumps(fn, args, kwargs)
        payload['cmd'] = 'python -m epos.execute {}'.format(callback)
        return start(payload=payload)

    return wrapper

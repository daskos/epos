from __future__ import absolute_import, division, print_function

from copy import copy
from functools import wraps

import os
import requests
from toolz import curry
from .execute import loads, dumps
from .utils import http_endpoint


default_host = os.environ.get('MARATHON_HOST')
endpoint = http_endpoint(host='{}/v2'.format(default_host))

start = endpoint(resource='/apps', method=requests.post)
destroy = endpoint(resource='/apps/{name}', method=requests.delete)
restart = endpoint(resource='/apps/{name}/restart', method=requests.post)

task = endpoint(resource='/apps/{name}/tasks/{id}')
tasks = endpoint(resource='/apps/{name}/tasks')


@curry
def marathon(fn, name=None, cpus=1, mem=512, instances=1, image='python'):
    payload = {
        'id': name or fn.__name__,
        'cpus': float(cpus),
        'mem': float(mem),
        'instances': int(instances)
    }
    if image:
        payload['container'] = {'docker': {'image': str(image)}}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        callback = dumps(fn, args, kwargs)
        payload['cmd'] = 'python -m epos.execute {}'.format(callback)
        return start(payload=payload)

    return wrapper

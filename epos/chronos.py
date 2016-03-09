from __future__ import absolute_import, division, print_function

from functools import wraps

import os
import requests
from toolz import curry
from .execute import dumps, command
from .utils import http_endpoint


default_host = os.environ.get('CHRONOS_HOST')
endpoint = http_endpoint(host='{}/scheduler'.format(default_host))

schedule_job = endpoint(resource='/iso8601', method=requests.post)
depend_job = endpoint(resource='/dependency', method=requests.post)
jobs = endpoint(resource='/jobs', method=requests.get)

start = endpoint(resource='/job/{name}', method=requests.put)
destroy = endpoint(resource='/job/{name}', method=requests.delete)


@curry
def chronos(fn, name=None, cpus=1, mem=512, image='python',
            schedule=None, parents=[], path='$PYTHONPATH', uris=[]):
    payload = {'name': fn.__name__, 'cpus': str(cpus), 'mem': str(mem),
               'disabled': False, 'uris': uris}

    if schedule:
        payload['schedule'] = schedule
    elif parents:
        payload['parents'] = parents

    if image:
        payload['container'] = {'type': 'DOCKER',
                                'image': image, 'forcePullImage': True}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload['command'] = command(fn, args, kwargs, path=path)
        return schedule_job(payload=payload)

    return wrapper

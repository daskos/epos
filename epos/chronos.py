from __future__ import absolute_import, division, print_function

from functools import wraps

import os
import requests
from toolz import curry
from .execute import command
from .utils import http_endpoint, envargs


default_host = os.environ.get('CHRONOS_HOST')
endpoint = http_endpoint(host='{}/scheduler'.format(default_host))

schedule_job = endpoint(resource='/iso8601', method=requests.post)
depend_job = endpoint(resource='/dependency', method=requests.post)

jobs = endpoint(resource='/jobs', method=requests.get)
start = endpoint(resource='/job/{job}', method=requests.put)
delete = endpoint(resource='/job/{job}', method=requests.delete)

destroy = endpoint(resource='/task/kill/{job}', method=requests.delete)

@curry
@envargs(prefix='EPOS_CHRONOS_')
def chronos(fn, name=None, cpus=0.1, mem=128, image='python', force_pull=True,
            schedule=None, parents=[], path='$PYTHONPATH', uris=[], envs=[],
            retries=2, disabled=False, async=False):
    payload = {'name': fn.__name__,
               'cpus': str(cpus),
               'async': bool(async),
               'mem': str(mem),
               'disabled': disabled,
               'retries': int(retries),
               'uris': uris,
               'environmentVariables': envs}

    if schedule:
        payload['schedule'] = schedule
    elif parents:
        payload['parents'] = parents

    if image:
        payload['container'] = {'type': 'DOCKER',
                                'image': str(image),
                                'forcePullImage': bool(force_pull)}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload['command'] = command(fn, args, kwargs, path=path)
        return schedule_job(payload=payload)

    return wrapper

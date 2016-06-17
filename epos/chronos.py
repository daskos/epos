from __future__ import absolute_import, division, print_function

from functools import wraps

import requests
from toolz import curry
from .execute import command
from .utils import http_endpoint
from .context import envargs


schedule_job = http_endpoint(resource='/scheduler/iso8601',
                             method=requests.post)
depend_job = http_endpoint(resource='/scheduler/dependency',
                           method=requests.post)

jobs = http_endpoint(resource='/scheduler/jobs', method=requests.get)
start = http_endpoint(resource='/scheduler/job/{job}', method=requests.put)
delete = http_endpoint(resource='/scheduler/job/{job}', method=requests.delete)

destroy = http_endpoint(resource='/scheduler/task/kill/{job}',
                        method=requests.delete)


@curry
@envargs(prefix='CHRONOS')
def chronos(fn, name=None, cpus=0.1, mem=128, docker='lensa/epos',
            force_pull=False, schedule=None, parents=[], path='$PYTHONPATH',
            uris=[], envs={}, retries=2, disabled=False, async=False,
            host='localhost:4400'):
    """Cronos job launcher"""
    envs = [{'name': k, 'value': v} for k, v in envs.items()]
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

    if docker:
        payload['container'] = {'type': 'DOCKER',
                                'image': str(docker),
                                'forcePullImage': bool(force_pull)}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload['command'] = command(fn, args, kwargs, path=path)
        return schedule_job(host=host, payload=payload)

    return wrapper

from __future__ import absolute_import, division, print_function

from six import wraps

import requests
from toolz import curry
from dask.delayed import tokenize

from .execute import command
from .utils import http_endpoint
from .context import lazyargs


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
@lazyargs
def chronos(fn, name=None, cpus=0.1, mem=128, docker='lensa/epos',
            force_pull=False, schedule=None, parents=[], path='$PYTHONPATH',
            uris=[], envs={}, retries=2, disabled=False, async=False,
            host='localhost:4400'):
    """Cronos job launcher"""
    @wraps(fn, assigned=('__name__', '__doc__'))
    def wrapper(*args, **kwargs):
        environs = [{'name': k, 'value': v} for k, v in envs.items()]
        payload = {'cpus': str(cpus),
                   'async': bool(async),
                   'mem': str(mem),
                   'disabled': bool(disabled),
                   'retries': int(retries),
                   'uris': list(uris),
                   'environmentVariables': environs}
        if schedule:
            payload['schedule'] = str(schedule)
        elif parents:
            payload['parents'] = list(parents)
        if docker:
            payload['container'] = {'type': 'DOCKER',
                                    'image': str(docker),
                                    'forcePullImage': bool(force_pull)}
        cid = '{}-{}'.format(name or fn.__name__, tokenize(*args, **kwargs))
        payload['name'] = cid
        payload['command'] = command(fn, args, kwargs, path=path)
        schedule_job(host=host, payload=payload)
        return cid

    return wrapper

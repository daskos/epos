import os
import sys
import requests
import inspect
from setuptools.sandbox import run_setup
import shutil
from toolz import curry
from functools import wraps
from copy import copy


@curry
def http_endpoint(host, resource, method=requests.get, **params):
    endpoint = 'http://{host}/{resource}'.format(host=host,
                                                 resource=resource.lstrip('/'))
    headers = {'Content-Type': 'application/json'}

    @wraps(method)
    def wrapper(payload={}, **params):
        url = endpoint.format(**params)
        response = method(url, json=payload, headers=headers,
                          allow_redirects=True)
        response.raise_for_status()
        try:
            return response.json()
        except:
            return response

    return wrapper


@curry
def envargs(fn, prefix='', envs=os.environ):
    spec = inspect.getargspec(fn)
    envs = {k: envs[prefix + k.upper()] for k in spec.args
            if prefix + k.upper() in envs}
    defs = dict(zip(spec.args[-len(spec.defaults):],
                    spec.defaults))
    defs.update(envs)

    @wraps(fn)
    def wrapper(*args, **kwargs):
        params = copy(defs)
        params.update(zip(spec.args[:len(args)], args))
        params.update(kwargs)
        return fn(**params)

    return wrapper


def locate_package(fn):
    pkg_name = fn.__module__.split('.')[0]
    pkg_path = os.path.dirname(sys.modules[pkg_name].__file__)
    return os.path.abspath(pkg_path)


def zip_package(pkg_path, format='zip', root_dir='/tmp'):
    return shutil.make_archive(pkg_path, format=format, root_dir=root_dir)


def egg_package(pkg_path, root_dir='/tmp'):
    setup_path = os.path.dirname(pkg_path) + '/setup.py'
    run_setup(setup_path, ['sdist', '-d', root_dir])

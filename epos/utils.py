from __future__ import absolute_import, division, print_function

import os
import sys
import shutil
import requests

from toolz import curry
from setuptools.sandbox import run_setup


MB, MiB = 1, 1
GB, GiB = 1000 * MB, 1024 * MiB
TB, TiB = 1000 * GB, 1024 * GiB


@curry
def http_endpoint(host, resource, method=requests.get, payload={},
                  scheme='http', extract=None, **params):
    endpoint = '{scheme}://{host}/{resource}'.format(
        host=host, scheme=scheme, resource=resource.lstrip('/'))
    headers = {'Content-Type': 'application/json'}

    url = endpoint.format(**params)
    response = method(url, json=payload, headers=headers,
                      allow_redirects=True)
    response.raise_for_status()
    try:
        data = response.json()
        if extract:
            return extract(data)
        else:
            return data
    except:
        return response


def locate_package(fn):
    pkg_name = fn.__module__.split('.')[0]
    pkg_path = os.path.dirname(sys.modules[pkg_name].__file__)
    return os.path.abspath(pkg_path)


def zip_package(pkg_path, format='zip', root_dir='/tmp'):
    return shutil.make_archive(pkg_path, format=format, root_dir=root_dir)


def egg_package(pkg_path, root_dir='/tmp'):
    setup_path = os.path.dirname(pkg_path) + '/setup.py'
    run_setup(setup_path, ['sdist', '-d', root_dir])

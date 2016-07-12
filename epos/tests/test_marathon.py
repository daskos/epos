from __future__ import print_function, absolute_import, division

import os
import time
import pytest

from epos.marathon import marathon, destroy, deployments, app, apps
from satyr.utils import timeout


host = os.environ.get('MARATHON_HOST')
pytestmark = pytest.mark.skipif(
    not host, reason="MARATHON_HOST environment variable must be set")


@pytest.yield_fixture(autouse=True)
def destroy_apps():
    try:
        yield
    finally:
        for a in apps(host=host):
            destroy(id=a['id'], host=host)

        with timeout(15):
            while len(deployments(host=host)):
                time.sleep(.1)

    assert len(apps(host=host)) == 0


def test_marathon():
    uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
    pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'

    @marathon(docker=None, cpus=0.1, mem=64, path=pythonpath, uris=uris,
              host=host)
    def test(a, b):
        while True:
            time.sleep(0.1)
            print('Slept 0.1s')

    mid = test(1, 2)
    with timeout(20):
        while len(deployments(host=host)):
            time.sleep(.1)

    result = app(id=mid, host=host)
    assert result['tasksRunning'] == 1


def test_marathon_docker():
    @marathon(cpus=0.1, mem=64, host=host)
    def docker(a, b):
        while True:
            time.sleep(0.1)
            print('Slept 0.1s')

    mid = docker(1, 2)
    with timeout(20):
        while len(deployments(host=host)):
            time.sleep(.1)

    result = app(id=mid, host=host)
    assert result['tasksRunning'] == 1

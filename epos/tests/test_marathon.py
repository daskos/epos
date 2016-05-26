from __future__ import print_function, absolute_import, division

import pytest
import time
from epos.marathon import marathon, destroy, deployments, app, apps
from satyr.utils import timeout

# TODO skip if not hdfs
# TODO skip if not marathon


@pytest.fixture(scope='module', autouse=True)
def destroy_apps():
    for a in apps()['apps']:
        destroy(id=a['id'])

    with timeout(15):
        while len(deployments()):
            time.sleep(.1)


def test_marathon_start():
    uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
    pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'

    @marathon(docker=None, cpus=0.1, mem=64, path=pythonpath, uris=uris)
    def test(a, b):
        while True:
            time.sleep(0.1)
            print('Slept 0.1s')

    try:
        test(1, 2)
        with timeout(20):
            while len(deployments()):
                time.sleep(.1)

        result = app(id='test')
        assert result['app']['tasksRunning'] == 1
    finally:
        destroy(id='test')

    assert len(apps()['apps']) == 0


def test_marathon_docker_start():
    @marathon(docker='lensa/epos:dev', cpus=0.1, mem=64)
    def docker(a, b):
        while True:
            time.sleep(0.1)
            print('Slept 0.1s')

    try:
        docker(1, 2)
        with timeout(20):
            while len(deployments()):
                time.sleep(.1)

        result = app(id='docker')
        assert result['app']['tasksRunning'] == 1
    finally:
        destroy(id='docker')

    assert len(apps()['apps']) == 0

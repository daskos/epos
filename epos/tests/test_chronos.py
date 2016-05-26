from __future__ import print_function, absolute_import, division

import pytest
import time

from epos.chronos import chronos, start, delete, destroy, jobs
from satyr.utils import timeout


@pytest.fixture(scope='module', autouse=True)
def destroy_jobs():
    for job in jobs():
        destroy(job=job['name'])
        delete(job=job['name'])
    with timeout(10):
        while len(jobs()):
            time.sleep(.1)


@pytest.mark.skip(reason='cannot test without docker due to colon separated '
                         'naming of chronos jobs')
def test_chronos_start():
    uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
    pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'

    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M',
             cpus=0.1, mem=128, uris=uris, path=pythonpath)
    def test(a, b):
        print('Test function')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(job='test')

        with timeout(20):
            while not jobs()[0]['successCount']:
                time.sleep(.1)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(job='test')
        delete(job='test')

    assert len(jobs()) == 0


def test_chronos_docker_start():
    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M', docker='lensa/epos:dev',
             cpus=0.1, mem=128)
    def test(a, b):
        print('Test function')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(job='test')

        with timeout(20):
            while not jobs()[0]['successCount']:
                time.sleep(.1)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(job='test')
        delete(job='test')

    assert len(jobs()) == 0

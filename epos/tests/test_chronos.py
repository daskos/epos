from __future__ import print_function, absolute_import, division

import pytest
import time

from epos.chronos import chronos, start, delete, destroy, jobs
from satyr.utils import timeout


# TODO test w/o docker



@pytest.fixture(scope='module', autouse=True)
def destroy_jobs():
    for job in jobs():
        destroy(job=job['name'])
        delete(job=job['name'])
    with timeout(10):
        while len(jobs()):
            sleep(.5)


@pytest.mark.skip(reason='cannot test without docker due to colon separated '
                         'naming of chronos jobs')
def test_chronos_start():
    uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
    pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'

    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M',
             cpus=0.1, mem=64, uris=uris, path=pythonpath)
    def test(a, b):
        print('Sleeping or 2s')
        time.sleep(2)
        print('Slept 2s')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(job='test')

        with timeout(20):
            while not jobs()[0]['successCount']:
                time.sleep(.5)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(job='test')
        delete(job='test')

    assert len(jobs()) == 0


def test_chronos_docker_start():
    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M', image='lensa/satyr',
             cpus=0.1, mem=64)
    def test(a, b):
        print('Sleeping or 2s')
        time.sleep(2)
        print('Slept 2s')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(job='test')

        with timeout(20):
            while not jobs()[0]['successCount']:
                time.sleep(.5)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(job='test')
        delete(job='test')

    assert len(jobs()) == 0

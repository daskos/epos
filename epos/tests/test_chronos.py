from __future__ import print_function, absolute_import, division

import pytest
from epos.chronos import chronos, start, delete, destroy, jobs
from time import sleep


# TODO test w/o docker

uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'


@pytest.fixture(scope='module', autouse=True)
def destroy_jobs():
    for job in jobs():
        destroy(job=job['name'])
        delete(job=job['name'])
    while len(jobs()):
        sleep(.5)


def test_chronos_start():
    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M', image='python:2-alpine',
             mem=16, uris=uris, path=pythonpath)
    def test(a, b):
        print('Sleeping or 2s')
        sleep(2)
        print('Slept 2s')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(job='test')

        i = 0
        while (i < 50) and not jobs()[0]['successCount']:
            i += 1
            sleep(.5)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(job='test')
        delete(job='test')

    assert len(jobs()) == 0

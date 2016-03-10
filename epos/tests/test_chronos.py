from __future__ import print_function, absolute_import, division

import pytest
from epos.chronos import chronos, start, destroy, jobs
from time import sleep


# TODO test w/o docker

uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'


@pytest.fixture(scope='module', autouse=True)
def destroy_jobss():
    print(jobs())
    for job in jobs():
        destroy(name=job['name'])


def test_chronos_start():
    @chronos(schedule='R/2015-01-01T20:00Z/PT1M', image='python:2-alpine',
             uris=uris, path=pythonpath)
    def test(a, b):
        print('Sleeping or 2s')
        sleep(2)
        print('Slept 2s')

    try:
        test(1, 2)  # add job
        assert jobs()[0]['name'] == 'test'

        start(name='test')
        while not jobs()[0]['successCount']:
            sleep(.5)

        assert jobs()[0]['successCount'] == 1
    finally:
        destroy(name='test')

    assert len(jobs()) == 0

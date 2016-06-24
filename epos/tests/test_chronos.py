from __future__ import print_function, absolute_import, division

import os
import time
import pytest

from epos.chronos import chronos, start, delete, destroy, jobs
from satyr.utils import timeout


host = os.environ.get('CHRONOS_HOST')
pytestmark = pytest.mark.skipif(
    not host, reason="CHRONOS_HOST environment variable must be set")


@pytest.yield_fixture(autouse=True)
def destroy_jobs():
    try:
        yield
    finally:
        for job in jobs(host=host):
            destroy(job=job['name'], host=host)
            delete(job=job['name'], host=host)
        with timeout(10):
            while len(jobs(host=host)):
                time.sleep(.1)
    assert len(jobs(host=host)) == 0


@pytest.mark.skip(reason='cannot test without docker due to colon separated '
                         'naming of chronos jobs')
def test_chronos():
    uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
    pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'

    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M',
             cpus=0.1, mem=128, uris=uris, path=pythonpath)
    def test(a, b):
        print('Test function')

    cid = test(1, 2)  # add job
    assert jobs()[0]['name'] == 'test'

    start(job=cid)

    with timeout(20):
        while not jobs()[0]['successCount']:
            time.sleep(.1)

    assert jobs()[0]['successCount'] == 1


def test_chronos_docker():
    @chronos(schedule='R0/2015-01-01T20:00Z/PT1M', docker='lensa/epos:dev',
             cpus=0.1, mem=128, host=host)
    def test(a, b):
        print('Test function')

    cid = test(1, 2)  # add job

    assert jobs(host=host)[0]['name'] == cid

    start(job=cid, host=host)

    with timeout(20):
        while not jobs(host=host)[0]['successCount']:
            time.sleep(.1)

    assert jobs(host=host)[0]['successCount'] == 1

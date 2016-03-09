from __future__ import print_function, absolute_import, division

from epos.chronos import chronos, start, destroy, jobs
from time import sleep


# TODO test w/o docker

uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'


def test_chronos_start():
    @chronos(image='python:2-alpine', schedule='R/2015-01-01T20:00Z/PT1M',
             cpus=0.1, mem=512, uris=uris, path=pythonpath)
    def test(a, b):
        print('Sleeping or 5s')
        sleep(5)
        print('Slept 5s')

    test(1, 2)  # add job
    assert jobs()[0]['name'] == 'test'

    start(name='test')
    while not jobs()[0]['successCount']:
        sleep(.5)
    assert jobs()[0]['successCount'] == 1

    destroy(name='test')
    assert len(jobs()) == 0

from __future__ import print_function, absolute_import, division

import pytest
from epos.chronos import chronos, start, destroy, jobs
from time import sleep


def test_chronos_start():
    @chronos(image=None, schedule='R0/2015-01-01T20:00Z/PT30M', cpus=0.1, mem=64)
    def test(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    test(1, 2)
    assert jobs()[0]['name'] == 'test'

    start(name='test')
    sleep(10)
    assert jobs()[0]['successCount'] == 1

    destroy(name='test')
    assert len(jobs()) == 0

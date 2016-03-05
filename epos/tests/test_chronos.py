from __future__ import print_function, absolute_import, division

import pytest
from epos.chronos import chronos, start, destroy, jobs
from epos.execute import loads, dumps, run
from time import sleep


def test_chronos_start():
    @chronos(image=None, schedule='R/2100-01-01T20:00:00.000Z/PT1Y')
    def test(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    response = test(1, 2)
    print(response)

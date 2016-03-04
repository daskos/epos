from __future__ import print_function, absolute_import, division

import os
import pytest
from epos.marathon import marathon, destroy, deployments, app
from epos.execute import loads, dumps, run
from time import sleep


drone_dir = os.environ.get('DRONE_DIR')
import_path = '{dd}:{dd}/.eggs/cloudpickle-0.2.1-py2.7.egg'.format(
    dd=drone_dir)


# @pytest.yield_fixture(autouse=True)
# def marathon_cleanup():
#     try:
#         yield
#     finally:
#         destroy(id='test')


def test_marathon_start():
    @marathon(image=None, envs={'PYTHONPATH': import_path})
    def test(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    response = test(1, 2)
    while len(deployments()):
        sleep(.5)

    result = app(id='test')
    assert result['app']['tasksRunning'] == 1

    destroy(id='test')

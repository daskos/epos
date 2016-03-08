from __future__ import print_function, absolute_import, division

import os
import pytest
from requests import HTTPError
from epos.marathon import marathon, destroy, deployments, app
from epos.execute import loads, dumps, run
from time import sleep

# TODO skip if not hdfs
# TODO skip if not marathon

uris = ['https://github.com/cloudpipe/cloudpickle/archive/v0.2.1.tar.gz']
pythonpath = '$MESOS_SANDBOX/cloudpickle-0.2.1'


def test_marathon_start():
    @marathon(image=None, path=pythonpath, uris=uris)
    def test(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    try:
        response = test(1, 2)
        while len(deployments()):
            sleep(.5)

        result = app(id='test')
        assert result['app']['tasksRunning'] == 1
    finally:
        destroy(id='test')


def test_marathon_docker_start():
    @marathon(image='python:2-alpine', path=pythonpath, uris=uris)
    def docker(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    try:
        response = docker(1, 2)
        while len(deployments()):
            sleep(.5)

        result = app(id='docker')
        assert result['app']['tasksRunning'] == 1
    finally:
        destroy(id='docker')

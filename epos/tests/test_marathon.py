from __future__ import print_function, absolute_import, division

import pytest
from epos.marathon import marathon, destroy, tasks
from epos.execute import loads, dumps, run
from time import sleep


#@pytest.fixture(autouse=True)
# def proxy_payload(monkeypatch):
#    monkeypatch.setattr('requests.post', lambda host, json: json)


def test_marathon_start():
    @marathon(image=None)
    def test(a, b):
        while True:
            sleep(5)
            print('Slept 5s')

    response = test(1, 2)
    destroy('test')

    tasks = tasks('test')

    assert response.status_code == 201
    assert len(tasks['tasks']) == 1

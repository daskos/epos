import os
from epos.utils import envargs


def calc(a, b, c='c', d='d'):
    return a + b + c + d

def proxy(a, b, **kwargs):
    return (a, b, kwargs)


def test_local_args():
    fn = envargs(calc, envs={'C': 'C'})
    assert fn('a', 'b') == 'abCd'
    assert fn('b', 'a') == 'baCd'

    fn = envargs(calc, envs={'B': 'B'})
    assert fn('a') == 'aBcd'
    assert fn('a', 'b') == 'abcd'

    fn = envargs(calc, envs={'B': 'B', 'D': 'D'})
    assert fn('a') == 'aBcD'
    assert fn('a', 'b') == 'abcD'
    assert fn('a', d='d') == 'aBcd'
    assert fn('a', b='b', c='/', d='d') == 'ab/d'


def test_kwargs():
    envs = {'FN_TEST': 3, 'FN_A': 5, 'FN_ASD': 50, 'RRRR': 90}
    fn = envargs(proxy, envs=envs, prefix='FN_')

    assert fn(b=2) == (5, 2, {'test': 3, 'asd': 50})
    assert fn(b=2, asd=20) == (5, 2, {'test': 3, 'asd': 20})
    assert fn(a=1, b=3, test=12) == (1, 3, {'test': 12, 'asd': 50})


def test_osenv_args():
    os.environ['B'] = 'B'
    os.environ['D'] = 'D'

    fn = envargs(calc)
    assert fn('a') == 'aBcD'
    assert fn('a', 'b') == 'abcD'
    assert fn('a', d='d') == 'aBcd'
    assert fn('a', b='b', c='/', d='d') == 'ab/d'

    del os.environ['B']
    del os.environ['D']


def test_prefixed_osenv_args():
    os.environ['EPOS_TEST_B'] = 'B'
    os.environ['EPOS_TEST_D'] = 'D'

    fn = envargs(calc, prefix='EPOS_TEST_')
    assert fn('a') == 'aBcD'
    assert fn('a', 'b') == 'abcD'
    assert fn('a', d='d') == 'aBcd'
    assert fn('a', b='b', c='/', d='d') == 'ab/d'

    del os.environ['EPOS_TEST_B']
    del os.environ['EPOS_TEST_D']

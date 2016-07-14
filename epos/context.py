from __future__ import absolute_import, division, print_function

import inspect
from copy import copy
from toolz import curry
from six import wraps
from functools import partial
from collections import defaultdict
from cloudpickle import CloudPickler

from .lazy import Proxy

_globals = defaultdict(lambda: None)


class set_options(object):
    """Set global state within controled context

    This lets you specify various global settings in a tightly controlled with
    block

    Valid keyword arguments currently include:

        get - the scheduler to use
        pool - a thread or process pool
        cache - Cache to use for intermediate results
        func_loads/func_dumps - loads/dumps functions for serialization of data
            likely to contain functions.  Defaults to
            cloudpickle.loads/cloudpickle.dumps
        rerun_exceptions_locally - rerun failed tasks in master process

    Examples
    --------

    >>> with set_options(get=dask.get):  # doctest: +SKIP
    ...     x = np.array(x)  # uses dask.get internally
    """

    def __init__(self, **kwargs):
        self.old = _globals.copy()
        _globals.update(kwargs)

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        _globals.clear()
        _globals.update(self.old)


def getter(name, k, d):
    return _globals.get(name, {}).get(k, d)


@curry
def lazyargs(fn):
    try:
        spec = inspect.getargspec(fn)
        func = fn
    except TypeError:
        spec = inspect.getargspec(fn.func)
        func = fn.func

    defaults = spec.defaults or tuple()
    args = spec.args[-len(defaults):]
    params = [partial(getter, name=fn.__name__, k=args[k], d=d)
              for k, d in enumerate(defaults)]
    # fn = wraps(fn, assigned=('__name__', '__doc__'))
    func.__defaults__ = tuple(map(Proxy, params))
    return fn


@curry
def options(fn, key=None, data={}):
    key = key or fn.__name__
    try:
        spec = inspect.getargspec(fn)
    except TypeError:  # curried
        spec = inspect.getargspec(fn.func)
    defaults = spec.defaults or []
    kwds = dict(zip(spec.args[-len(defaults):], defaults))

    @wraps(fn, assigned=('__name__', '__doc__'))
    def closure(*args, **kwargs):
        envs = data or _globals[key] or {}  # get values just before calling
        if not spec.keywords:  # if fn doesn't have kwargs
            envs = {k: v for k, v in envs.items() if k in spec.args}
        params = copy(kwds)
        params.update(envs)
        params.update(zip(spec.args[:len(args)], args))
        params.update(kwargs)
        return fn(**params)

    return closure


@curry
def envargs(fn, prefix='', envs={}):
    envs = envs or _globals['envs'] or {}
    if len(prefix):
        prefix += '_'  # MARATHON => MARATHON_
    envs = {k[len(prefix):].lower(): v
            for k, v in envs.items() if k.startswith(prefix)}
    return options(fn, data=envs)


def inject_addons(self):
    self.save_reduce(lambda opts: set_options(**opts), (_globals,))

# register reducer to auto pickle _globals configuration
CloudPickler.inject_addons = inject_addons

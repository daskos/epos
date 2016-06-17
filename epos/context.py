from __future__ import absolute_import, division, print_function

import inspect
from copy import copy
from toolz import curry
from functools import wraps
from collections import defaultdict

_globals = defaultdict(lambda: None)


class set_options(object):
    """ Set global state within controled context

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


@curry
def envargs(fn, prefix='', envs={}):
    envs = envs or _globals['envs'] or {}
    if len(prefix):
        prefix += '_'  # MARATHON => MARATHON_
    try:
        spec = inspect.getargspec(fn)
    except TypeError:  # curried
        spec = inspect.getargspec(fn.func)
    envs = {k[len(prefix):].lower(): v
            for k, v in envs.items() if k.startswith(prefix)}
    if not spec.keywords:
        envs = {k: v for k, v in envs.items() if k in spec.args}
    defaults = spec.defaults or []
    defs = dict(zip(spec.args[-len(defaults):], defaults))
    defs.update(envs)

    @wraps(fn)
    def closure(*args, **kwargs):
        params = copy(defs)
        params.update(zip(spec.args[:len(args)], args))
        params.update(kwargs)
        return fn(**params)

    return closure

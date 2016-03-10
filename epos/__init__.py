from __future__ import absolute_import, division, print_function

from contextlib import contextmanager


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


with ignoring(ImportError):
    from .context import Lock, Persist

with ignoring(ImportError):
    from .spark import spark

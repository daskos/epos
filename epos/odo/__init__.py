from __future__ import absolute_import, division, print_function

from odo.utils import ignoring

from .core import Iterator

with ignoring(ImportError):
    from .bason import BSON

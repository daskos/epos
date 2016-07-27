from __future__ import absolute_import, division, print_function

from epos.odo.core import iterator_to_chunked_iterator
from toolz import first
from collections import Iterator


def test_iterator_to_chunked_iterator():
    it = (i for i in range(100))
    chunked = iterator_to_chunked_iterator(it, chunksize=5)

    i = 0
    for f in chunked:
        assert isinstance(f, Iterator)
        assert tuple(f) == tuple(range(i, i + 5))
        i += 5

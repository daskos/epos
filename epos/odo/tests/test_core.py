from __future__ import absolute_import, division, print_function

from epos.odo.core import iterator_to_chunked_iterator
from toolz import first


def test_iterator_to_chunked_iterator():
    it = (i for i in range(100))
    chunked = iterator_to_chunked_iterator(it, chunksize=5)

    assert first(chunked) == tuple(range(5))
    assert len(list(chunked)) == 19

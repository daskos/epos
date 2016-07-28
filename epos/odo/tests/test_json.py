from __future__ import absolute_import, division, print_function

from epos.odo.json import iterator_to_temporary_jsonlines
from toolz import first
from collections import Iterator
from odo.temp import _Temp

def test_iterator_to_temporary_jsonlines():
    it = (i for i in range(100))
    jl = iterator_to_temporary_jsonlines(it)
    assert isinstance(jl, _Temp)

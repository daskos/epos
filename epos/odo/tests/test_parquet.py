from __future__ import absolute_import, division, print_function

from odo import resource
from epos.odo.parquet import Parquet


def test_resource():
    assert isinstance(resource('foo.parquet'), Parquet)
    assert isinstance(resource('/path/to/foo.parquet'), Parquet)

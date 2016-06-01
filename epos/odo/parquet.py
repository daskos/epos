from __future__ import absolute_import, division, print_function

from odo import resource


class Parquet(object):
    canonical_extension = 'parquet'

    def __init__(self, path, **kwargs):
        self.path = path


@resource.register('.+\.(parquet)?')
def resource_parquet(uri, **kwargs):
    return Parquet(uri)

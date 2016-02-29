from odo import append, resource


class Parquet(object):
    canonical_extension = 'parquet'

    def __init__(self, path, **kwargs):
        print "PARQUET {}".format(path)
        self.path = path


@resource.register('.+\.(parquet)?')
def resource_parquet(uri, **kwargs):
    return Parquet(uri)

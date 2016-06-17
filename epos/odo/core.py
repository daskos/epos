from collections import Iterator

from odo import convert
from odo.chunks import chunks
from toolz import partition_all


@convert.register(chunks(Iterator), Iterator, cost=0.1)
def iterator_to_chunked_iterator(it, chunksize=2**20, **kwargs):
    return chunks(Iterator)(partition_all(chunksize, it))


@convert.register(chunks(list), Iterator, cost=0.1)
def iterator_to_chunked_list(it, chunksize=2**20, **kwargs):
    return chunks(list)(partition_all(chunksize, it))

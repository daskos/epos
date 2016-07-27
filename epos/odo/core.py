from collections import Iterator
import itertools

from odo import convert
from odo.chunks import chunks
from toolz import partition_all


def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


@convert.register(chunks(Iterator), Iterator, cost=0.1)
def iterator_to_chunked_iterator(it, chunksize=2**20, **kwargs):
    return chunks(Iterator)(grouper_it(chunksize, it))


@convert.register(chunks(list), Iterator, cost=0.1)
def iterator_to_chunked_list(it, chunksize=2**20, **kwargs):
    return chunks(list)(partition_all(chunksize, it))

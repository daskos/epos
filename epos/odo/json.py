from __future__ import absolute_import, division, print_function

import uuid
from odo import JSONLines, Temp, convert, append
from collections import Iterator


@convert.register(Temp(JSONLines), Iterator)
def iterator_to_temporary_jsonlines(data, **kwargs):
    fn = '.%s.json' % uuid.uuid1()
    target = Temp(JSONLines)(fn)
    return append(target, data, **kwargs)

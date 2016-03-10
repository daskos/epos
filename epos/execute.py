from __future__ import print_function, absolute_import, division

import sys
import base64
import cloudpickle


bash = ("import sys, base64, cloudpickle; "
        "fn, args, kwargs = cloudpickle.loads(base64.b64decode(sys.argv[1])); "
        "fn(*args, **kwargs)")


def dumps(fn, args=[], kwargs={}):
    payload = (fn, args, kwargs)
    pickled = cloudpickle.dumps(payload, protocol=2)
    return base64.b64encode(pickled)


def loads(encoded):
    pickled = base64.b64decode(encoded)
    payload = cloudpickle.loads(pickled)
    return payload


def run(serialized):
    fn, args, kwargs = loads(serialized)
    return fn(*args, **kwargs)


def command(fn, args, kwargs, path='$PYTHONPATH'):
    callback = dumps(fn, args, kwargs)
    return "PYTHONPATH={path} python -c '{code}' {arg}".format(path=path,
                                                               code=bash,
                                                               arg=callback)


if __name__ == '__main__':
    run(sys.argv[1])

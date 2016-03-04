import os
import sys
import base64
import cloudpickle


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


if __name__ == '__main__':
    print(run(sys.argv[1]))

import requests
from toolz import curry
from functools import wraps


@curry
def http_endpoint(host, resource, method=requests.get, **params):
    endpoint = 'http://{host}/{resource}'.format(host=host,
                                                 resource=resource.lstrip('/'))

    @wraps(method)
    def wrapper(payload={}, **params):
        print(payload)
        url = endpoint.format(**params)
        response = method(url, json=payload)
        response.raise_for_status()
        return response.json()

    return wrapper

try:
    from .spark import spark
    from .context import Lock, Persist
except:
    pass

__all__ = ('spark',
           'Lock',
           'Persist')


import threading


def daemon(f):
    '''
    a threading decorator
    use @background above the function you want to run in the background
    '''
    def bg_f(*a, **kw):
        thread = threading.Thread(target=f, args=a, kwargs=kw)
        thread.daemon = True
        thread.start()
    return bg_f

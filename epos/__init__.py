try:
    from .spark import spark
    from .context import Lock, Persist
except:
    pass

__all__ = ('spark',
           'Lock',
           'Persist')

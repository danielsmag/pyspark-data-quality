from __future__ import annotations

import threading
from functools import wraps

def singleton(cls):
    _instance = None
    _lock = threading.RLock()

    @wraps(cls)
    def _get_instance(*args, **kwargs):
        nonlocal _instance
        if _instance is not None:
            return _instance
        with _lock:
            if _instance is None:
                _instance = cls(*args, **kwargs)
        return _instance

    return _get_instance

"""Caching utilities and decorators"""

from functools import wraps
from flask import request


def request_cache(func):
    """Decorator that caches function results within a single request"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not hasattr(request, '_cache'):
            request._cache = {}

        # Create a cache key from function name and arguments
        key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

        if key not in request._cache:
            request._cache[key] = func(*args, **kwargs)

        return request._cache[key]

    return wrapper
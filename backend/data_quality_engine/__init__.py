try:
    from .version import __version__
except ImportError:
    __version__ = "0.1.0"  # Default version if file not found
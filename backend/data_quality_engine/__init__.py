try:
    from .version import __version__
except ImportError:
    __version__ = "0.3.3"  # Default version if file not found
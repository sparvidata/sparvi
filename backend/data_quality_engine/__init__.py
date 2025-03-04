try:
    from .version import __version__
except ImportError:
    __version__ = "0.3.5"  # Default version if file not found
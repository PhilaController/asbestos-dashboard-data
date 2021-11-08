from pathlib import Path

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

__version__ = version(__package__)

DATA_DIR = Path(__file__).parent.resolve() / ".." / "data"

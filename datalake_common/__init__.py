import pyver

__version__, __version_info__ = pyver.get_version(pkg='datalake-common')
__all__ = ['Metadata']

from metadata import *

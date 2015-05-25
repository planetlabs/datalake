import pyver

__version__, __version_info__ = pyver.get_version(pkg='allthelogs')
__all__ = ['log']

from log import *

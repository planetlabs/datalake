import pyver

__version__, __version_info__ = pyver.get_version(pkg='allthelogs')
__all__ = ['Log', 'Archive', 'Metadata']

from metadata import *
from log import Log
from archive import Archive

import pyver

__version__, __version_info__ = pyver.get_version(pkg='datalake')
__all__ = ['File', 'Archive', 'Metadata']

from metadata import *
from dlfile import File
from archive import Archive

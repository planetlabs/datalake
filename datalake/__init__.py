import pyver

__version__, __version_info__ = pyver.get_version(pkg='datalake')
__all__ = ['File', 'Archive']

from dlfile import File
from archive import Archive

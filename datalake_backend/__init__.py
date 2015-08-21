import pyver

__version__, __version_info__ = pyver.get_version(pkg='datalake_backend')

from storage import DynamoDBStorage

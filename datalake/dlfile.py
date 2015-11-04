import os
from memoized_property import memoized_property
from pyblake2 import blake2b

from datalake_common import Metadata


class File(object):
    '''A File to be manipulated by the Archive'''

    def __init__(self, path, **metadata_fields):
        '''Create a File

        Args:

            path: path to the file

            metadata_fields: known metadata fields that go with this
            file. Missing fields will be added if they can be
            determined. Othwerise, InvalidDatalakeMetadata will be raised.

        '''
        self._fd = open(path, 'r')
        self._path = os.path.abspath(path)
        self._initialize_methods_from_fd()
        self._infer_missing_metadata_fields(metadata_fields)
        self.metadata = Metadata(metadata_fields)

    def _initialize_methods_from_fd(self):
        for m in ['read', 'readlines', 'seek', 'tell', 'close']:
            setattr(self, m, getattr(self._fd, m))

    def _infer_missing_metadata_fields(self, metadata_fields):
        if 'hash' not in metadata_fields:
            # do not recalculate the hash if it is already known
            metadata_fields['hash'] = self._calculate_hash()

        default_where = os.environ.get('DATALAKE_DEFAULT_WHERE')
        where = metadata_fields.get('where')
        if where is None and default_where is not None:
            metadata_fields['where'] = default_where

    _HASH_BUF_SIZE = 65536

    def _calculate_hash(self):
        '''16-byte blake2b hash over the content of this file'''
        # this takes just under 2s on my laptop for a 1GB file.
        b2 = blake2b(digest_size=16)
        with open(self._path, 'rb') as f:
            while True:
                data = f.read(self._HASH_BUF_SIZE)
                if not data:
                    break
                b2.update(data)
        return b2.hexdigest()

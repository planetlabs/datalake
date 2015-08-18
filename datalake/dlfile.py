from metadata import Metadata
import os
from memoized_property import memoized_property
from pyblake2 import blake2b


class File(object):
    '''A File to be manipulated by the Archive

    A File is a file and its associated metadata. To create a new File, you
    must provide the path to the file and the required Metadata.
    '''
    def __init__(self, path, metadata):
        if type(metadata) is not Metadata:
            metadata = Metadata(metadata)
        self.metadata = metadata
        self._fd = open(path, 'r')
        self._path = os.path.abspath(path)
        self._basename = os.path.basename(path)
        self._initialize_methods_from_fd()
        self.metadata['hash'] = self.hash

    def _initialize_methods_from_fd(self):
        for m in ['read', 'readlines', 'seek', 'tell', 'close']:
            setattr(self, m, getattr(self._fd, m))

    _HASH_BUF_SIZE = 65536

    def _calculate_hash(self):
        # this takes just under 2s on my laptop for a 1GB file.
        b2 = blake2b(digest_size=16)
        with open(self._path, 'rb') as f:
            while True:
                data = f.read(self._HASH_BUF_SIZE)
                if not data:
                    break
                b2.update(data)
        return b2.hexdigest()

    @memoized_property
    def hash(self):
        '''16-byte blake2b hash over the content of this file'''
        return self._calculate_hash()

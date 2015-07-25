from metadata import Metadata
import os


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
        self.name = os.path.basename(path)
        self._initialize_methods_from_fd()

    def _initialize_methods_from_fd(self):
        for m in ['read', 'readlines', 'seek', 'tell', 'close']:
            setattr(self, m, getattr(self._fd, m))

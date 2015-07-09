from metadata import Metadata


class Log(file):

    @classmethod
    def from_atl_metadata(cls, metadata, path):
        if type(metadata) is not Metadata:
            metadata = Metadata(metadata)
        log = cls(path)
        log.metadata = metadata
        log.path = path
        return log

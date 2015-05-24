
class InvalidATLMetadata(Exception):
    pass


class UnsupportedATLMetadataVersion(Exception):
    pass


class Log(object):

    @classmethod
    def from_atl_metadata(cls, metadata):
        cls._validate_metadata(metadata)
        log = cls()
        log.metadata = metadata
        return log

    @classmethod
    def _validate_metadata(cls, metadata):
        cls._validate_required_fields(metadata)
        cls._validate_version(metadata['version'])

    _REQUIRED_METADATA_FIELDS = ['version', 'start', 'end', 'where', 'what']

    @classmethod
    def _validate_required_fields(cls, metadata):
        for f in cls._REQUIRED_METADATA_FIELDS:
            if f not in metadata:
                msg = '%s is a require field'.format(f)
                raise InvalidATLMetadata(msg)

    @classmethod
    def _validate_version(cls, version):
        if version != '0':
            msg = 'Found version %s. Only "0" is supported'.format(version)
            raise UnsupportedATLMetadataVersion(msg)

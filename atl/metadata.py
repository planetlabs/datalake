from copy import deepcopy
from dateutil.parser import parse as dateparse


class InvalidATLMetadata(Exception):
    pass


class UnsupportedATLMetadataVersion(Exception):
    pass


class Metadata(dict):

    def __init__(self, *args, **kwargs):
        # we want to own all of our bits so we can normalize them without
        # altering the caller's data unexpectedly. So deepcopy.
        args = deepcopy(args)
        kwargs = deepcopy(kwargs)
        super(Metadata, self).__init__(*args, **kwargs)
        self._validate()
        self._normalize_dates()

    def _validate(self):
        self._validate_required_fields()
        self._validate_version()

    _REQUIRED_METADATA_FIELDS = ['version', 'start', 'end', 'where', 'what']

    def _validate_required_fields(self):
        for f in self._REQUIRED_METADATA_FIELDS:
            if f not in self:
                msg = '%s is a require field'.format(f)
                raise InvalidATLMetadata(msg)

    def _validate_version(self):
        v = self['version']
        if v != '0':
            msg = 'Found version %s. Only "0" is supported'.format(v)
            raise UnsupportedATLMetadataVersion(msg)

    def _normalize_dates(self):
        for d in ['start', 'end']:
            date = self._normalize_date(self[d])
            self[d] = date.isoformat() + 'Z'

    @staticmethod
    def _normalize_date(date):
        try:
            return dateparse(date)
        except ValueError:
            msg = 'could not parse a date from %s'.format(date)
            raise InvalidATLMetadata(msg)

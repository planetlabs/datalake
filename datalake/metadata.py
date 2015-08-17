from copy import deepcopy
from dateutil.parser import parse as dateparse
from datetime import datetime
from pytz import utc
from uuid import uuid4


class InvalidDatalakeMetadata(Exception):
    pass


class UnsupportedDatalakeMetadataVersion(Exception):
    pass

_EPOCH = datetime.fromtimestamp(0, utc)

class Metadata(dict):

    def __init__(self, *args, **kwargs):
        # we want to own all of our bits so we can normalize them without
        # altering the caller's data unexpectedly. So deepcopy.
        args = deepcopy(args)
        kwargs = deepcopy(kwargs)
        super(Metadata, self).__init__(*args, **kwargs)
        self._add_id()
        self._validate()
        self._normalize_dates()

    def _add_id(self):
        self['id'] = uuid4().hex

    def _validate(self):
        self._validate_required_fields()
        self._validate_version()

    _REQUIRED_METADATA_FIELDS = ['version', 'start', 'end', 'where', 'what']

    def _validate_required_fields(self):
        for f in self._REQUIRED_METADATA_FIELDS:
            if f not in self:
                msg = '"{}" is a require field'.format(f)
                raise InvalidDatalakeMetadata(msg)

    def _validate_version(self):
        v = self['version']
        if v != '0':
            msg = 'Found version {}. Only "0" is supported'.format(v)
            raise UnsupportedDatalakeMetadataVersion(msg)

    def _normalize_dates(self):
        for d in ['start', 'end']:
            self[d] = self._normalize_date(self[d])

    @staticmethod
    def _normalize_date(date):
        if type(date) is int:
            return date
        elif type(date) is float:
            return int(date * 1000.0)
        else:
            return Metadata._normalize_date_from_string(date)

    @staticmethod
    def _normalize_date_from_string(date):
        try:
            d = dateparse(date)
            if not d.tzinfo:
                d = d.replace(tzinfo=utc)
            return Metadata._datetime_to_milliseconds(d)
        except ValueError:
            msg = 'could not parse a date from {}'.format(date)
            raise InvalidDatalakeMetadata(msg)

    @staticmethod
    def _datetime_to_milliseconds(d):
        delta = d - _EPOCH
        return int(delta.total_seconds()*1000.0)

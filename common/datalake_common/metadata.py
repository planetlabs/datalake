# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from copy import deepcopy
from dateutil.parser import parse as dateparse
from datetime import datetime
from pytz import utc
from uuid import uuid4
import re
import simplejson as json
from simplejson.scanner import JSONDecodeError
import os
import pytz

# as seconds: 5138-11-16 09:46:40
# as milliseconds: 1973-03-03 09:46:40
MAX_TS_SECONDS = 100000000000

try:
    long = long
except NameError:
    # Python3
    long = int
    basestring = str


class InvalidDatalakeMetadata(Exception):
    pass


class UnsupportedDatalakeMetadataVersion(Exception):
    pass


_EPOCH = datetime.fromtimestamp(0, utc)


_WINDOWS_ABS_PATH = re.compile(r'^[a-zA-Z]:\\.+')


class Metadata(dict):

    _VERSION = 0

    def __init__(self, *args, **kwargs):
        '''prepare compliant, normalized metadata from inputs

        Args:

            kwargs: key-value pairs for metadata fields.

        Raises:

            InvalidDatalakeMetadata if required fields are missing and cannot
            be inferred.
        '''
        # we want to own all of our bits so we can normalize them without
        # altering the caller's data unexpectedly. So deepcopy.
        args = deepcopy(args)
        kwargs = deepcopy(kwargs)
        super(Metadata, self).__init__(*args, **kwargs)
        self._ensure_id()
        self._ensure_version()
        self._validate()
        self._normalize_dates()
        self._validate_interval()  # must occur after normalizing

    @classmethod
    def from_json(cls, j):
        if j is None:
            raise InvalidDatalakeMetadata('None is not a valid JSON')
        try:
            return cls(json.loads(j))
        except JSONDecodeError:
            msg = '{} is not valid json'.format(repr(j))
            raise InvalidDatalakeMetadata(msg)

    @property
    def json(self):
        return json.dumps(self)

    def _ensure_id(self):
        if 'id' not in self:
            self['id'] = uuid4().hex

    def _ensure_version(self):
        if 'version' not in self:
            self['version'] = self._VERSION

    def _validate(self):
        self._validate_required_fields()
        self._validate_version()
        self._validate_slug_fields()
        self._validate_work_id()
        self._validate_path()

    _REQUIRED_METADATA_FIELDS = ['version', 'start', 'where', 'what', 'id',
                                 'hash', 'path']

    def _validate_required_fields(self):
        for f in self._REQUIRED_METADATA_FIELDS:
            if self.get(f) is None:
                msg = '"{}" is a required field'.format(f)
                raise InvalidDatalakeMetadata(msg)

    def _validate_version(self):
        v = self['version']
        if v != self._VERSION:
            msg = ('Found version {}. '
                   'Only {} is supported').format(v, self._VERSION)
            raise UnsupportedDatalakeMetadataVersion(msg)

    _SLUG_FIELDS = ['where', 'what']

    def _validate_slug_fields(self):
        [self._validate_slug_field(f) for f in self._SLUG_FIELDS]

    def _validate_slug_field(self, f):
        if not re.match(r'^[a-z0-9_-]+$', self[f]):
            msg = ('Invalid value "{}" for "{}". Only lower-case letters, '
                   '_ and - are allowed.').format(self[f], f)
            raise InvalidDatalakeMetadata(msg)

    def _validate_slug_field_with_dots(self, f):
        if not re.match(r'^[\.a-z0-9_-]+$', self[f]):
            msg = ('Invalid value "{}" for "{}". Only lower-case letters, '
                   'underscores, dashes, and dots '
                   'are allowed.').format(self[f], f)
            raise InvalidDatalakeMetadata(msg)

    def _validate_work_id(self):
        if 'work_id' not in self:
            msg = '"work_id" is required, but it can be None'
            raise InvalidDatalakeMetadata(msg)

        if self['work_id'] is None:
            return
        self._validate_slug_field('work_id')
        if self['work_id'] == 'null':
            msg = '"work_id" cannot be the string "null"'
            raise InvalidDatalakeMetadata(msg)

    def _validate_path(self):
        if not os.path.isabs(self['path']) and \
           not self._is_windows_abs(self['path']):
            msg = '{} is not an absolute path.'.format(self['path'])
            raise InvalidDatalakeMetadata(msg)

    def _is_windows_abs(self, path):
        return _WINDOWS_ABS_PATH.match(path) is not None

    def _validate_interval(self):
        end_val = self['end']
        if end_val is None:
            return
        if end_val < self['start']:
            msg = '"end" must be greater than "start"'
            raise InvalidDatalakeMetadata(msg)

    def _normalize_dates(self):
        self['start'] = self.normalize_date(self['start'])
        self._normalize_end()

    def _normalize_end(self):
        end_val = self.setdefault('end', None)
        if end_val is not None:
            self['end'] = self.normalize_date(end_val)

    @staticmethod
    def normalize_date(date):
        '''normalize the specified date to milliseconds since the epoch

        If it is a string, it is assumed to be some sort of datetime such as
        "2015-12-27" or "2015-12-27T11:01:20.954". If date is a naive datetime,
        it is assumed to be UTC.

        If numeric arguments are beyond 5138-11-16 (100,000,000,000 seconds
        after epoch), they are interpreted as milliseconds since the epoch.
        '''

        if isinstance(date, datetime):
            pass
        elif date == "now":
            date = datetime.now(pytz.UTC)
        elif isinstance(date, (basestring, int, float, long)):
            try:
                ts = float(date)
                if ts > MAX_TS_SECONDS:
                    # ts was provided in ms
                    ts = ts / 1000.0
                # For unix timestamps on command line
                date = datetime.utcfromtimestamp(float(ts))
            except ValueError:
                try:
                    date = dateparse(date)
                except ValueError as e:
                    raise InvalidDatalakeMetadata(str(e))
        else:
            msg = 'could not parse a date from {!r}'.format(date)
            raise InvalidDatalakeMetadata(msg)

        return Metadata._from_datetime(date)

    @staticmethod
    def _from_datetime(date):
        if not date.tzinfo:
            date = date.replace(tzinfo=utc)
        return Metadata._datetime_to_milliseconds(date)

    @staticmethod
    def _datetime_to_milliseconds(d):
        delta = d - _EPOCH
        return int(delta.total_seconds()*1000.0)

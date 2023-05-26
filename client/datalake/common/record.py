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

from . import Metadata
from six.moves.urllib.parse import urlparse
import json
import os


from .errors import InsufficientConfiguration, UnsupportedTimeRange, \
    NoSuchDatalakeFile

'''whether or not s3 features are available

Users may wish to check if s3 features are available before invoking them. If
they are unavailable, the affected functions will raise
InsufficientConfiguration.'''
has_s3 = True
try:
    import boto3
except ImportError:
    has_s3 = False


def requires_s3(f):
    def wrapped(*args, **kwargs):
        if not has_s3:
            msg = 'This feature requires s3 features.  '
            msg += '`pip install datalake-common[s3]` to turn this feature on.'
            raise InsufficientConfiguration(msg)
        return f(*args, **kwargs)
    return wrapped


# NB: Some time ago we migrated this class from datalake-common to datalake in
# order to reduce the number of packages that comprise the datalake. As a side
# effect of this migration this class contains some code duplicated with the
# datalake.Archive. There is an opportunity to clean this up, but we can take
# that on after getting off of boto 2.
class DatalakeRecord(dict):

    def __init__(self, url, metadata, time_bucket, create_time, size):
        self.metadata = metadata
        parts = {
            'version': 0,
            'url': url,
            'time_index_key': '{}:{}'.format(time_bucket, metadata['what']),
            'work_id_index_key': self._get_work_id_index_key(),
            'range_key': self._get_range_key(),
            'create_time': create_time,
            'size': size,
            'metadata': metadata,
        }
        super(DatalakeRecord, self).__init__(parts)

    @classmethod
    @requires_s3
    def list_from_url(cls, url):
        '''return a list of DatalakeRecords for the specified url'''
        obj, metadata = cls._get_object(url)
        ct = cls._get_create_time(obj)
        time_buckets = cls.get_time_buckets_from_metadata(metadata)

        return [
            cls(url, metadata, t, ct, obj.content_length) for t in time_buckets
        ]

    @classmethod
    @requires_s3
    def list_from_metadata(cls, url, metadata):
        '''return a list of DatalakeRecords for the url and metadata'''
        obj, _ = cls._get_object(url)
        metadata = Metadata(**metadata)
        ct = cls._get_create_time(obj)
        time_buckets = cls.get_time_buckets_from_metadata(metadata)
        return [
            cls(url, metadata, t, ct, obj.content_length) for t in time_buckets
        ]

    @classmethod
    def _get_create_time(cls, obj):
        return Metadata.normalize_date(obj.last_modified)

    @classmethod
    def _get_object(cls, url):
        parsed_url = urlparse(url)
        bucket = parsed_url.netloc

        # NB: under boto 2 we didn't used to have to have the lstrip. It seems
        # that boto2 explicitly stripped these leading slashes for us:
        # https://groups.google.com/g/boto-users/c/mv--NMPUXoU ...but boto3
        # does not. So we must take care to strip it whenever we parse a URL to
        # get a key.
        key_name = parsed_url.path.lstrip('/')
        obj = cls._connection().Object(parsed_url.netloc, key_name)

        try:
            # cache the results of the get on the obj to avoid superfluous
            # network calls.
            obj._datalake_details = obj.get()
            m = obj._datalake_details['Metadata'].get('datalake')
        except cls._connection().meta.client.exceptions.NoSuchKey:
            msg = '{} does not appear to be in the datalake'
            msg = msg.format(url)
            raise NoSuchDatalakeFile(msg)
        except cls._connection().meta.client.exceptions.NoSuchBucket:
            msg = 'Cannot find datalake file (s3 bucket {} does not exist)'
            msg = msg.format(bucket)
            raise NoSuchDatalakeFile(msg)

        return obj, Metadata.from_json(m)

    _CONNECTION = None

    @classmethod
    def _connection(cls):
        if cls._CONNECTION is None:
            cls._CONNECTION = cls._prepare_connection()
        return cls._CONNECTION

    @classmethod
    def _s3_host(cls):
        h = os.environ.get('AWS_S3_HOST')
        if h is not None:
            return 'https://' + h
        r = (os.environ.get('AWS_REGION') or
             os.environ.get('AWS_DEFAULT_REGION'))
        if r is not None:
            return 'https://s3-' + r + '.amazonaws.com'
        else:
            return None

    @classmethod
    def _prepare_connection(cls):

        return boto3.resource('s3',
                              region_name=os.environ.get('AWS_REGION'),
                              endpoint_url=cls._s3_host())

    _ONE_DAY_IN_MS = 24*60*60*1000

    '''The size of a time bucket in milliseconds

    Each datalake record appears once in each time bucket that it covers. For
    example, suppose the time buckets are one day long. If a record has a start
    time of 1994-01-01T00:00:00 and an end time of 1994-01-03T02:33:29, it will
    appear in the bucket for 1994-01-01, 1994-01-02, and 1994-01-03.
    '''
    TIME_BUCKET_SIZE_IN_MS = _ONE_DAY_IN_MS

    '''The maximum number of buckets that a record is allowed to span

    A major weakness of our time-based indexing scheme is that it duplicates
    records into each relevant time bucket. In practice, we do not expect files
    that span more than a few buckets. So if a file spans many many buckets,
    let's assume something went wrong.
    '''
    MAXIMUM_BUCKET_SPAN = 30

    @staticmethod
    def get_time_buckets_from_metadata(metadata):
        '''return a list of time buckets in which the metadata falls'''
        start = metadata['start']
        end = metadata.get('end') or start
        buckets = DatalakeRecord.get_time_buckets(start, end)
        if len(buckets) > DatalakeRecord.MAXIMUM_BUCKET_SPAN:
            msg = 'metadata spans too many time buckets: {}'
            j = json.dumps(metadata)
            msg = msg.format(j)
            raise UnsupportedTimeRange(msg)
        return buckets

    @staticmethod
    def get_time_buckets(start, end):
        '''get the time buckets spanned by the start and end times'''
        d = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
        first_bucket = start / d
        last_bucket = end / d
        return list(range(
            int(first_bucket),
            int(last_bucket) + 1))

    def _get_range_key(self):
        return self.metadata['where'] + ':' + self.metadata['id']

    def _get_work_id_index_key(self):
        work_id = self.metadata['work_id'] or 'null' + self.metadata['id']
        return work_id + ':' + self.metadata['what']

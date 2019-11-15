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

from . import Metadata, InvalidDatalakeMetadata
from six.moves.urllib.parse import urlparse
import simplejson as json
import os


from .errors import InsufficientConfiguration, UnsupportedTimeRange, \
    NoSuchDatalakeFile

'''whether or not s3 features are available

Users may wish to check if s3 features are available before invoking them. If
they are unavailable, the affected functions will raise
InsufficientConfiguration.'''
has_s3 = True
try:
    import boto.s3
    from boto.exception import S3ResponseError
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
        key = cls._get_key(url)
        metadata = cls._get_metadata_from_key(key)
        ct = cls._get_create_time(key)
        time_buckets = cls.get_time_buckets_from_metadata(metadata)
        return [cls(url, metadata, t, ct, key.size) for t in time_buckets]

    @classmethod
    @requires_s3
    def list_from_metadata(cls, url, metadata):
        '''return a list of DatalakeRecords for the url and metadata'''
        key = cls._get_key(url)
        metadata = Metadata(**metadata)
        ct = cls._get_create_time(key)
        time_buckets = cls.get_time_buckets_from_metadata(metadata)
        return [cls(url, metadata, t, ct, key.size) for t in time_buckets]

    @classmethod
    def _get_create_time(cls, key):
        return Metadata.normalize_date(key.last_modified)

    @classmethod
    def _get_key(cls, url):
        parsed_url = urlparse(url)
        bucket = cls._get_bucket(parsed_url.netloc)
        key = bucket.get_key(parsed_url.path)
        if key is None:
            msg = '{} does not appear to be in the datalake'
            msg = msg.format(url)
            raise NoSuchDatalakeFile(msg)
        return key

    @classmethod
    def _get_metadata_from_key(cls, key):
        metadata = key.get_metadata('datalake')
        if not metadata:
            msg = 'No datalake metadata for s3://{}{}'
            msg = msg.format(key.bucket.name, key.name)
            raise InvalidDatalakeMetadata(msg)
        return Metadata.from_json(metadata)

    _BUCKETS = {}

    @classmethod
    def _get_bucket(cls, bucket_name):
        if bucket_name not in cls._BUCKETS:
            bucket = cls._get_bucket_from_s3(bucket_name)
            DatalakeRecord._BUCKETS[bucket_name] = bucket
        return cls._BUCKETS[bucket_name]

    @classmethod
    def _get_bucket_from_s3(cls, bucket_name):
        try:
            return cls._connection().get_bucket(bucket_name)
        except S3ResponseError as e:
            if e.error_code == 'NoSuchBucket':
                msg = 'Cannot find datalake file (s3 bucket {} does not exist)'
                msg = msg.format(bucket_name)
                raise NoSuchDatalakeFile(msg)
            else:
                raise

    _CONNECTION = None

    @classmethod
    def _connection(cls):
        if cls._CONNECTION is None:
            cls._CONNECTION = cls._prepare_connection()
        return cls._CONNECTION

    @classmethod
    def _prepare_connection(cls):
        kwargs = {}
        s3_host = os.environ.get('AWS_S3_HOST')
        if s3_host:
            kwargs['host'] = s3_host
        return boto.connect_s3(**kwargs)

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

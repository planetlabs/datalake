import boto.s3
from boto.s3.key import Key
from memoized_property import memoized_property
import simplejson as json
from datalake_common import Metadata
from urlparse import urlparse

from conf import get_config
from errors import InvalidS3Notification, InvalidS3Event


class S3ToDatalakeTranslator(object):

    def translate(self, s3_notification):
        '''translate an s3 notification into datalake records

        s3 notifications coming from SNS have the format described here:
        http://docs.aws.amazon.com/sns/latest/dg/json-formats.html#http-notification-json

        From these we must pull out the Message, which is an S3 event and has
        the format described here:

        http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html

        Returns a list of all of the DatalakeRecords associated with all of the
        files reported in the s3 events.

        '''
        msg = s3_notification.get('Message')
        if msg is None:
            raise InvalidS3Notification(json.dumps(s3_notification))
        s3_event = S3Event(msg)
        return list(self._datalake_records_from_s3_event(s3_event))

    def _datalake_records_from_s3_event(self, s3_event):
        for s3r in s3_event.records:
            for dlr in self._datalake_records_from_s3_record(s3r):
                yield dlr

    def _datalake_records_from_s3_record(self, s3_record):
        if not s3_record['eventName'].startswith('ObjectCreated'):
            raise StopIteration()
        for dlr in DatalakeRecord.list_from_url(s3_record.url):
            yield dlr


class DatalakeRecord(dict):

    def __init__(self, url, metadata, time_bucket):
        self.metadata = metadata
        parts = {
            'version': 0,
            'url': url,
            'time_index_key': '{}:{}'.format(time_bucket, metadata['what']),
            'work_id_index_key': self._get_work_id_index_key(),
            'range_key': self._get_range_key(),
            'metadata': metadata,
        }
        super(DatalakeRecord, self).__init__(parts)        


    @classmethod
    def list_from_url(cls, url):
        '''return a list of DatalakeRecords for the specified url'''
        metadata = cls._get_metadata(url)
        time_buckets = cls.get_time_buckets(metadata)
        return [cls(url, metadata, t) for t in time_buckets]

    @classmethod
    def _get_metadata(cls, url):
        parsed_url = urlparse(url)
        bucket = cls._get_bucket(parsed_url.netloc)
        key = bucket.get_key(parsed_url.path)
        metadata = key.get_metadata('datalake')
        if not metadata:
            raise InvalidS3Event('No datalake metadata for ' + url)
        return Metadata.from_json(metadata)

    _BUCKETS = {}

    @classmethod
    def _get_bucket(cls, bucket_name):
        if bucket_name not in cls._BUCKETS:
            bucket = cls._connection().get_bucket(bucket_name)
            DatalakeRecord._BUCKETS[bucket_name] = bucket
        return cls._BUCKETS[bucket_name]

    _CONNECTION = None

    @classmethod
    def _connection(cls):
        if cls._CONNECTION is None:
            cls._CONNECTION = cls._prepare_connection()
        return cls._CONNECTION

    @classmethod
    def _prepare_connection(cls):
        kwargs = {}
        s3_host = get_config().s3_host
        if s3_host:
            kwargs['host'] = s3_host
        return boto.connect_s3(**kwargs)

    _ONE_DAY_IN_MS = 24*60*60*1000

    @staticmethod
    def get_time_buckets(metadata):
        '''return a list of time buckets in which the metadata falls'''
        start = metadata['start']
        end = metadata['end']
        d = DatalakeRecord._ONE_DAY_IN_MS
        num_buckets = (end - start)/d + 1
        return [(start + i * d)/d for i in xrange(num_buckets)]

    def _get_range_key(self):
        return self.metadata['where'] + ':' + self.metadata['id']

    def _get_work_id_index_key(self):
        work_id = self.metadata['work_id'] or 'null' + self.metadata['id']
        return work_id + ':' + self.metadata['what']


class S3Event(dict):

    def __init__(self, raw_event):
        self.raw_event = raw_event
        event = json.loads(raw_event)
        super(S3Event, self).__init__(event)
        self._validate()

    def _validate(self):
        self._validate_versions()

    def _validate_versions(self):
        [self._validate_version(r) for r in self.records]

    def _validate_version(self, record):
        v = record.get('eventVersion')
        if v is None:
            msg = 'No eventVersion: ' + self.raw_event
            raise InvalidS3Event(msg)

        if v != '2.0':
            msg = 'Unsupported event version: ' + self.raw_event
            raise InvalidS3Event(msg)

    @memoized_property
    def records(self):
        return [S3Record(r) for r in self['Records']]


class S3Record(dict):

    @property
    def url(self):
        return 's3://' + self.bucket_name + '/' + self.key_name

    @property
    def bucket_name(self):
        return self['s3']['bucket']['name']

    @property
    def key_name(self):
        return self['s3']['object']['key']

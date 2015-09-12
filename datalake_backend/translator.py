from memoized_property import memoized_property
import simplejson as json

from errors import InvalidS3Notification, InvalidS3Event
from record import DatalakeRecord


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
        if self.get('Event') == 's3:TestEvent':
            return []
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

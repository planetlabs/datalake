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

from memoized_property import memoized_property
import simplejson as json

from errors import InvalidS3Notification, InvalidS3Event
from datalake_common import DatalakeRecord


class S3Notification(dict):

    def __init__(self, s3_notification):
        self.message = s3_notification.get('Message')
        if self.message is None:
            raise InvalidS3Notification(self.message)
        self.message = json.loads(self.message)
        super(S3Notification, self).__init__(self.message)

    @memoized_property
    def events(self):
        if self.get('Event') == 's3:TestEvent':
            return []
        return [S3Event(r) for r in self.message['Records']]


class S3Event(dict):

    EVENTS_WITH_RECORDS = ['ObjectCreated:Put', 'ObjectCreated:Copy']

    def __init__(self, event):
        super(S3Event, self).__init__(event)
        self._validate()

    def _validate(self):
        self._validate_version()

    def _validate_version(self):
        v = self.get('eventVersion')
        if v is None:
            msg = 'No eventVersion: ' + json.dumps(self)
            raise InvalidS3Event(msg)

        if v != '2.0':
            msg = 'Unsupported event version: ' + json.dumps(self)
            raise InvalidS3Event(msg)

    @memoized_property
    def datalake_records(self):
        if self['eventName'] not in self.EVENTS_WITH_RECORDS:
            return []
        return [dlr for dlr in DatalakeRecord.list_from_url(self.s3_url)]

    @property
    def s3_url(self):
        return 's3://' + self.bucket_name + '/' + self.key_name

    @property
    def bucket_name(self):
        return self['s3']['bucket']['name']

    @property
    def key_name(self):
        return self['s3']['object']['key']

    @property
    def event_name(self):
        return self['eventName']

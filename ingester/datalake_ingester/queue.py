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
import boto.sqs
import simplejson as json
import logging
import os
from datalake_common.errors import InsufficientConfiguration


class SQSQueue(object):
    '''A queue that hears events on an SQS queue and translates them'''

    def __init__(self, queue_name, handler=None):
        self.queue_name = queue_name
        self.handler = handler
        self.logger = logging.getLogger(queue_name)

    @classmethod
    def from_config(cls):
        queue_name = os.environ.get('DATALAKE_INGESTION_QUEUE')
        if queue_name is None:
            raise InsufficientConfiguration('Please configure a queue')
        return cls(queue_name)

    def set_handler(self, h):
        self.handler = h

    @memoized_property
    def _queue(self):
        return self._connection.get_queue(self.queue_name)

    @memoized_property
    def _connection(self):
        region = os.environ.get('AWS_REGION')
        if region:
            return boto.sqs.connect_to_region(region)
        else:
            return boto.connect_sqs()

    _LONG_POLL_TIMEOUT = 20

    def drain(self, timeout=None):
        '''drain the queue of message, invoking the handler for each item
        '''
        long_poll_timeout = timeout or self._LONG_POLL_TIMEOUT
        while True:
            raw_msg = self._queue.read(wait_time_seconds=long_poll_timeout)
            if raw_msg is None:
                if timeout:
                    return
                else:
                    continue
            self._handle_raw_message(raw_msg)

    def _handle_raw_message(self, raw_msg):
        # eliminate newlines in raw message so it all logs to one line
        raw = raw_msg.get_body().replace('\n', ' ')
        if not self.handler:
            self.logger.error('NO HANDLER CONFIGURED: %s', raw)
            return

        self.logger.info('RECEIVED: %s', raw)
        msg = json.loads(raw)

        self.handler(msg)
        self._queue.delete_message(raw_msg)

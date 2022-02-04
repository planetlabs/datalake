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
import boto3
import simplejson as json
import logging
import os
from datalake.common.errors import InsufficientConfiguration

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
        return self._sqs.get_queue_by_name(QueueName=self.queue_name)

    @memoized_property
    def _sqs(self):
        # TODO: Migrate to AWS_DEFAULT_REGION and let the library handle it
        region = os.environ.get('AWS_REGION')
        if region:
            return boto3.resource('sqs', region_name=region)
        else:
            return boto3.resource('sqs')

    _LONG_POLL_TIMEOUT = 20

    def drain(self, timeout=None):
        '''drain the queue of message, invoking the handler for each item
        '''
        long_poll_timeout = timeout or self._LONG_POLL_TIMEOUT
        while True:
            messages = self._queue.receive_messages(
                    WaitTimeSeconds = long_poll_timeout,
                    MaxNumberOfMessages = 10
            )

            if len(messages) == 0 and timeout is not None:
                return  # Drained

            for msg in messages:
                self._handle_message(msg.body)
                msg.delete()

    def _handle_message(self, body):
        # eliminate newlines in raw message so it all logs to one line
        raw = body.replace('\n', ' ')
        if not self.handler:
            self.logger.error('NO HANDLER CONFIGURED: %s', raw)
            return

        self.logger.info('RECEIVED: %s', raw)
        msg = json.loads(raw)

        self.handler(msg)

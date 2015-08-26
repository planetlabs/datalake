from memoized_property import memoized_property
import boto.sqs
import simplejson as json

from conf import get_config

class SQSQueue(object):
    '''A queue that hears events on an SQS queue and translates them'''

    def __init__(self, queue_name, handler, translator=None):
        self.queue_name = queue_name
        self.translator = translator
        self.handler = handler

    @memoized_property
    def _queue(self):
        return self._connection.get_queue(self.queue_name)

    @memoized_property
    def _connection(self):
        region = get_config().aws_region
        return boto.connect_sqs(region=region)

    _LONG_POLL_TIMEOUT = 20

    def drain(self, timeout=None):
        '''drain the queue of message, invoking the handler for each item
        '''
        long_poll_timeout = timeout or self.LONG_POLL_TIMEOUT
        while True:
            raw_msg = self._queue.read(wait_time_seconds=long_poll_timeout)
            if raw_msg is None and timeout:
                return
            msg = json.loads(raw_msg.get_body())
            self.handler(msg)
            self._queue.delete_message(raw_msg)

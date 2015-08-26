import pytest
import time
import simplejson as json

from datalake_backend import SQSQueue


@pytest.fixture
def handler():

    class Handler(object):
        messages = []

        def __call__(self, msg):
            self.messages.append(msg)

    return Handler()


def test_sqs_queue_timeout(sqs_queue, handler):
    q = SQSQueue(sqs_queue.name, handler)
    start = time.time()
    q.drain(timeout=1)
    duration = time.time() - start
    error = abs(duration - 1.0)
    assert error < 0.1
    assert handler.messages == []


def test_sqs_queue_drain(sqs_queue, handler):
    q = SQSQueue(sqs_queue.name, handler)
    expected_msg = {'foo': 'bar'}
    msg = sqs_queue.new_message(json.dumps(expected_msg))
    sqs_queue.write(msg)
    q.drain(timeout=1)
    assert handler.messages == [expected_msg]
    handler.messages = []
    q.drain(timeout=1)
    assert handler.messages == []

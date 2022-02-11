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

import pytest
import time
import simplejson as json

from datalake_ingester import SQSQueue


@pytest.fixture
def handler():

    class Handler(object):
        messages = []

        def __call__(self, msg):
            self.messages.append(msg)

    return Handler()


def test_sqs_queue_timeout(bare_sqs_test_queue, handler, mock_region_environ):
    q = SQSQueue('test-queue', handler)
    start = time.time()
    q.drain(timeout=1)
    duration = time.time() - start
    error = abs(duration - 1.0)
    assert error < 0.1
    assert handler.messages == []


def test_sqs_queue_drain(bare_sqs_test_queue, handler, mock_region_environ):
    q = SQSQueue('test-queue', handler)
    expected_msg = {'foo': 'bar'}
    bare_sqs_test_queue.send_message(MessageBody=json.dumps(expected_msg))
    q.drain(timeout=1)
    assert handler.messages == [expected_msg]
    handler.messages = []
    q.drain(timeout=1)
    assert handler.messages == []

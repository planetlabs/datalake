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

import simplejson as json

from datalake_ingester import SNSReporter


def test_snsreporter_sends(sns_connection, sns_topic_arn, bare_sqs_queue):
    topic = sns_connection.Topic(sns_topic_arn)
    topic.subscribe(
        Protocol='sqs',
        Endpoint=bare_sqs_queue.attributes['QueueArn']
    )
    r = SNSReporter(sns_topic_arn)
    expected_msg = {'message': 'foo'}
    r.report(expected_msg)
    messages = bare_sqs_queue.receive_messages(MaxNumberOfMessages=1)
    msg = json.loads(messages[0].body)
    assert expected_msg == json.loads(msg['Message'])

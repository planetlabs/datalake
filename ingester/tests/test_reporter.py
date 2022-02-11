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

from moto import mock_sns

from datalake_ingester import SNSReporter

@mock_sns
def test_snsreporter_sends(sns, sns_topic_arn, bare_sqs_test_queue, mock_region_environ):
    topic = sns.create_topic(Name='foo')
    topic.subscribe(
        Protocol="sqs",
        Endpoint=bare_sqs_test_queue.attributes["QueueArn"]
    )

    r = SNSReporter(topic.arn)
    expected_msg = {'message': 'foo'}
    r.report(expected_msg)
    msg = json.loads(json.loads(bare_sqs_test_queue.receive_messages()[0].body)['Message'])
    assert expected_msg == msg

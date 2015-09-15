import pytest
import simplejson as json

from datalake_ingester import SNSReporter


def test_snsreporter_sends(sns_connection, sns_topic_arn, bare_sqs_queue):
    sns_connection.subscribe_sqs_queue(sns_topic_arn, bare_sqs_queue)
    r = SNSReporter(sns_topic_arn)
    expected_msg = {'message': 'foo'}
    r.report(expected_msg)
    msg = json.loads(bare_sqs_queue.read(1).get_body())
    assert expected_msg == msg

import pytest
from moto import mock_sns, mock_sqs
import boto.sns
import boto.sqs
import simplejson as json

from datalake_backend import SNSReporter


@pytest.fixture
def aws_connector(request):

    def create_connection(mocker, connector):
        mock = mocker()
        mock.start()

        def tear_down():
            mock.stop()
        request.addfinalizer(tear_down)

        return connector()

    return create_connection


@pytest.fixture
def sns_connection(aws_connector):
    return aws_connector(mock_sns, boto.connect_sns)


@pytest.fixture
def sns_topic_arn(sns_connection):
    topic = sns_connection.create_topic('foo')
    return topic['CreateTopicResponse']['CreateTopicResult']['TopicArn']


@pytest.fixture
def sqs_connection(aws_connector):
    return aws_connector(mock_sqs, boto.connect_sqs)


@pytest.fixture
def sqs_queue(sqs_connection):
    return sqs_connection.create_queue("test-queue")


def test_snsreporter_sends(sns_connection, sns_topic_arn, sqs_queue):
    sns_connection.subscribe_sqs_queue(sns_topic_arn, sqs_queue)
    r = SNSReporter(sns_topic_arn)
    expected_msg = {'message': 'foo'}
    r.report(expected_msg)
    msg = json.loads(sqs_queue.read(1).get_body())
    assert expected_msg == msg

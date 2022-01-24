import pytest
from moto import mock_sns, mock_sqs, mock_dynamodb2
import os
import simplejson as json
from glob import glob

import boto3

#from datalake.tests import *  # noqa

from datalake_ingester import SQSQueue


def _delete_table_if_exists(dynamodb, name):
    try:
        dynamodb.Table(name).delete()
    except dynamodb_client.exceptions.ResourceNotFoundException:
        return


@pytest.fixture
def dynamodb_table_maker(request):

    def table_maker(name, schema):
        dynamodb = boto3.resource('dynamodb', region='us-west-1')
        _delete_table_if_exists(dynamodb, name)
        table = dynamodb.create_table(
                  TableName=name,
                  KeySchema=schema,
                  ProvisionedThroughput={
                      'ReadCapacityUnits': 5,
                      'WriteCapacityUnits': 5
                  }
                )

        def tear_down():
            _delete_table_if_exists(dynamodb, name)
        request.addfinalizer(tear_down)

        return table

    return table_maker


@pytest.fixture
def dynamodb_users_table(dynamodb_table_maker):
    schema = [
        {
            'AttributeName': 'name',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'last_name',
            'KeyType': 'RANGE'
        }
    ]
    return dynamodb_table_maker('users', schema)


@pytest.fixture
def dynamodb_records_table(dynamodb_table_maker):
    schema = [
        {
            'AttributeName': 'time_index_key',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'range_key',
            'KeyType': 'RANGE'
        }
    ]
    return dynamodb_table_maker('records', schema)



@pytest.fixture
def sns_topic_arn():
    sns = boto3.client('sns')
    topic = sns.create_topic(Name='foo')
    return topic.TopicArn



@pytest.fixture
def bare_sqs_queue_maker():

    def maker(queue_name):
        sqs = boto3.resource('sqs')
        try:
            return sqs.get_queue_by_name(QueueName=queue_name)
        except sqs.meta.client.exceptions.QueueDoesNotExist:
            return sqs.create_queue(QueueName=queue_name)

    return maker


@pytest.fixture
def sqs_queue_maker(bare_sqs_queue_maker):

    def maker(queue_name):
        q = bare_sqs_queue_maker(queue_name)
        return SQSQueue(q.name)

    return maker


@pytest.fixture
def bare_sqs_queue(bare_sqs_queue_maker):
    return bare_sqs_queue_maker('test-queue')


@pytest.fixture
def sqs_queue(sqs_queue_maker):
    return sqs_queue_maker('test-queue')


@pytest.fixture
def sqs_sender(bare_sqs_queue_maker):

    def sender(msg, queue_name='test-queue'):
        q = bare_sqs_queue_maker(queue_name)
        q.send_message(MessageBody=json.dumps(msg))

    return sender


# we use quick-and-dirty declarative tests here. Specifically, each json file
# in the data/ directory contains a test specification. Each test specification
# is comprised of a number of event_specifications. Each event_specifications
# describes the required s3_files that must be present, the related
# s3_notifications that would be delivered to the ingester, and the expected
# outcomes (e.g., s3_notification_exception,
# expected_datalake_records). Finally, the expected_reports are the ingester
# reports that we expect to be emitted as a consequence of the
# event_specifications.


_here = os.path.abspath(os.path.dirname(__file__))
_test_data_path = os.path.join(_here, 'data')
_test_specs = glob(os.path.join(_test_data_path, '*.json'))


@pytest.fixture(params=_test_specs)
def event_test_driver(request, s3_file_from_metadata):

    def driver(event_tester):
        spec = json.load(open(request.param))
        for e in spec['event_specifications']:
            for f in e.get('s3_files', []):
                s3_file_from_metadata(f['url'], f.get('metadata'))
            event_tester(e)
        return spec.get('expected_reports')

    return driver

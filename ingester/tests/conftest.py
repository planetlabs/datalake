import pytest
from moto import mock_sns, mock_sqs, mock_dynamodb2
import os
import simplejson as json
from glob import glob

from datalake.tests import *  # noqa

import boto3
import botocore.exceptions


from datalake_ingester import SQSQueue


# If we run with proper AWS credentials they will be used
# This will cause moto to fail
# But more critically, may impact production systems
# So we test for real credentials and fail hard if they exist
sts = boto3.client('sts')
try:
    sts.get_caller_identity()
    pytest.exit("Real AWS credentials detected, aborting", 3)
except botocore.exceptions.NoCredentialsError:
    pass  # no credentials are good


def _delete_table_if_exists(dynamodb, name):
    try:
        dynamodb.Table(name).delete()
    except botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] != 'ResourceNotFoundException':
            raise

@pytest.fixture(scope="function")
def dynamodb():
    with mock_dynamodb2():
        d2 = boto3.resource('dynamodb', region_name='us-east-1')  # must also be set in tests
        yield d2

@pytest.fixture
def dynamodb_table_maker(request, dynamodb):

    def table_maker(name, key_schema, attribute_definitions):
        _delete_table_if_exists(dynamodb, name)
        table = dynamodb.create_table(
                  TableName=name,
                  KeySchema=key_schema,
                  AttributeDefinitions=attribute_definitions,
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
    definitions = [
        {
            'AttributeName': 'name',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'last_name',
            'AttributeType': 'S'
        }
    ]
    return dynamodb_table_maker('users', schema, definitions)


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
    definitions = [
        {
            'AttributeName': 'time_index_key',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'range_key',
            'AttributeType': 'S'
        }
    ]
    return dynamodb_table_maker('records', schema, definitions)

@pytest.fixture(scope="function")
def sns():
    with mock_sns():
        sns = boto3.resource('sns', region_name='us-east-1')  # must also be set in tests
        yield sns


@pytest.fixture
def sns_topic_arn(sns):
    topic = sns.create_topic(Name='foo')
    return topic.arn


@pytest.fixture(scope="function")
def sqs():
    with mock_sqs():
        sqs = boto3.resource('sqs', region_name='us-east-1')  # must also be set in tests
        yield sqs

@pytest.fixture
def bare_sqs_queue_maker(sqs):

    def maker(queue_name):
        try:
            return sqs.get_queue_by_name(QueueName=queue_name)
        except sqs.meta.client.exceptions.QueueDoesNotExist:
            return sqs.create_queue(QueueName=queue_name)

    return maker


@pytest.fixture
def sqs_queue_maker(bare_sqs_queue_maker):

    def maker(queue_name):
        q = bare_sqs_queue_maker(queue_name)
        return SQSQueue(queue_name)

    return maker


@pytest.fixture
def bare_sqs_test_queue(bare_sqs_queue_maker):
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

@pytest.fixture
def mock_region_environ(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")

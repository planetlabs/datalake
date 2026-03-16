import pytest
from moto import mock_aws
import os
import simplejson as json
from glob import glob
import boto3
from botocore.exceptions import ClientError

from datalake.tests import *  # noqa

from datalake_ingester import SQSQueue

def _delete_table_if_exists(conn, name):
    try:
        table = conn.Table(name)
        table.delete()
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            raise
    
@pytest.fixture
def dynamodb_connection():
    with mock_aws():
        yield boto3.resource('dynamodb', region_name='us-east-1')


@pytest.fixture
def dynamodb_table_maker(request, dynamodb_connection):

    def table_maker(name, key_schema, attribute_definitions):
        _delete_table_if_exists(dynamodb_connection, name)

        table = dynamodb_connection.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        table.meta.client.get_waiter('table_exists').wait(
            TableName=name,
            WaiterConfig={'Delay': 1, 'MaxAttempts': 30}
        )

        def tear_down():
            _delete_table_if_exists(dynamodb_connection, name)

        request.addfinalizer(tear_down)

        return table

    return table_maker



@pytest.fixture
def dynamodb_users_table(dynamodb_table_maker):
    key_schema = [
        {'AttributeName': 'name', 'KeyType': 'HASH'},
        {'AttributeName': 'last_name', 'KeyType': 'RANGE'}
    ]
    attribute_definitions = [
        {'AttributeName': 'name', 'AttributeType': 'S'},
        {'AttributeName': 'last_name', 'AttributeType': 'S'}
    ]
    return dynamodb_table_maker('users', key_schema, attribute_definitions)


@pytest.fixture
def dynamodb_records_table(dynamodb_table_maker):
    key_schema = [
        {'AttributeName': 'time_index_key', 'KeyType': 'HASH'},
        {'AttributeName': 'range_key', 'KeyType': 'RANGE'}
    ]
    attribute_definitions = [
        {'AttributeName': 'time_index_key', 'AttributeType': 'S'},
        {'AttributeName': 'range_key', 'AttributeType': 'S'}
    ]
    return dynamodb_table_maker('records', key_schema, attribute_definitions)


@pytest.fixture
def dynamodb_latest_table(dynamodb_table_maker):
    key_schema = [
        {'AttributeName': 'what_where_key', 'KeyType': 'HASH'}
    ]
    attribute_definitions = [
        {'AttributeName': 'what_where_key', 'AttributeType': 'S'}
    ]
    return dynamodb_table_maker('latest', key_schema, attribute_definitions)


@pytest.fixture
def sns_connection():
    with mock_aws():
        yield boto3.resource('sns', region_name='us-east-1')


@pytest.fixture
def sns_topic_arn(sns_connection):
    topic = sns_connection.create_topic(Name='foo')
    return topic.arn


@pytest.fixture
def sqs_connection():
    with mock_aws():
        yield boto3.resource('sqs', region_name='us-east-1')


@pytest.fixture
def bare_sqs_queue_maker(sqs_connection):

    def maker(queue_name):
        try:
            queue = sqs_connection.get_queue_by_name(QueueName=queue_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                queue = sqs_connection.create_queue(QueueName=queue_name)
            else:
                raise
        return queue

    return maker


@pytest.fixture
def sqs_queue_maker(bare_sqs_queue_maker):

    def maker(queue_name):
        q = bare_sqs_queue_maker(queue_name)
        return SQSQueue(queue_name)

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
        q.send_message(
            MessageBody=json.dumps(msg)
        )

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

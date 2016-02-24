import pytest
from moto import mock_sns, mock_sqs, mock_dynamodb2
import os
import simplejson as json
from glob import glob

from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from boto.dynamodb2.fields import HashKey, RangeKey
import boto.sns
import boto.sqs

from datalake_common.tests import *  # noqa

from datalake_ingester import SQSQueue


@pytest.fixture
def dynamodb_connection(aws_connector):
    return aws_connector(mock_dynamodb2,
                         lambda: boto.dynamodb2.connect_to_region('us-west-1'))


def _delete_table_if_exists(conn, name):
    try:
        table = Table(name, connection=conn)
        table.delete()
    except JSONResponseError as e:
        if e.status == 400 and e.error_code == 'ResourceNotFoundException':
            return
        raise e


@pytest.fixture
def dynamodb_table_maker(request, dynamodb_connection):

    def table_maker(name, schema):
        _delete_table_if_exists(dynamodb_connection, name)
        throughput = {'read': 5, 'write': 5}
        table = Table.create(name,
                             schema=schema,
                             throughput=throughput,
                             connection=dynamodb_connection)

        def tear_down():
            _delete_table_if_exists(dynamodb_connection, name)
        request.addfinalizer(tear_down)

        return table

    return table_maker


@pytest.fixture
def dynamodb_users_table(dynamodb_table_maker):
    schema = [HashKey('name'), RangeKey('last_name')]
    return dynamodb_table_maker('users', schema)


@pytest.fixture
def dynamodb_records_table(dynamodb_table_maker):
    schema = [HashKey('time_index_key'), RangeKey('range_key')]
    return dynamodb_table_maker('records', schema)


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
def bare_sqs_queue_maker(sqs_connection):

    def maker(queue_name):
        return sqs_connection.get_queue(queue_name) or \
            sqs_connection.create_queue(queue_name)

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
        msg = q.new_message(json.dumps(msg))
        q.write(msg)

    return sender


_here = os.path.abspath(os.path.dirname(__file__))
test_data_path = os.path.join(_here, 'data')

_s3_notification_path = os.path.join(test_data_path, 's3-notification-*.json')
all_s3_notification_specs = glob(_s3_notification_path)

_bad_s3_notification_path = os.path.join(test_data_path,
                                         'bad-s3-notification-*.json')
all_bad_s3_notification_specs = glob(_bad_s3_notification_path)


@pytest.fixture
def spec_maker(s3_file_from_metadata):

    def maker(spec_file):
        spec = json.load(open(spec_file))
        expected_records = spec['expected_datalake_records']
        [s3_file_from_metadata(d['url'], d['metadata'])
         for d in expected_records]
        return spec

    return maker

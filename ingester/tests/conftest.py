import pytest
from moto import (
    mock_sns_deprecated as mock_sns,
    mock_sqs_deprecated as mock_sqs,
    mock_dynamodb2_deprecated as mock_dynamodb2
)
import os
import simplejson as json
from glob import glob

from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from boto.dynamodb2.fields import HashKey, RangeKey
import boto.sns
import boto.sqs

from datalake.tests import *  # noqa

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
def dynamodb_latest_table(dynamodb_table_maker):
    schema = [HashKey('time_index_key'), RangeKey('range_key')]
    return dynamodb_table_maker('latest', schema)


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

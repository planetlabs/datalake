import pytest
from moto import mock_sns, mock_sqs, mock_s3
import os
import simplejson as json
from urlparse import urlparse

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from boto.dynamodb2.fields import HashKey, RangeKey
import boto.sns
import boto.sqs
import boto.s3
from boto.s3.key import Key


@pytest.fixture
def dynamodb_connection(request):
    conn = DynamoDBConnection(aws_access_key_id='foo',
                              aws_secret_access_key='bar',
                              host='localhost',
                              port=8000,
                              is_secure=False)
    
    # Fail fast if the local dynamodb server is down. This is a bit of a monkey
    # patch because this magic variable seems to override all configurables
    # (e.g., num_retries).
    conn.NumberRetries = 1

    def tear_down():
        conn.close()
    request.addfinalizer(tear_down)

    return conn


def _delete_table_if_exists(conn, name):
    try:
        table = Table(name, connection=conn)
        table.delete()
    except JSONResponseError as e:
        if e.status == 400 and e.error_code == 'ResourceNotFoundException':
            return
        raise e


@pytest.fixture
def dynamodb_table_maker(dynamodb_connection):

    def table_maker(name, schema):
        _delete_table_if_exists(dynamodb_connection, name)
        throughput = {'read': 5, 'write': 5}
        return Table.create(name,
                            schema=schema,
                            throughput=throughput,
                            connection=dynamodb_connection)
        return table
    return table_maker


@pytest.fixture
def dynamodb_users_table(dynamodb_table_maker):
    schema = [HashKey('name'), RangeKey('last_name')]
    return dynamodb_table_maker('users', schema)


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


_here = os.path.abspath(os.path.dirname(__file__))
test_data_path = os.path.join(_here, 'data')


@pytest.fixture
def s3_connection(aws_connector):
    return aws_connector(mock_s3, boto.connect_s3)


@pytest.fixture
def s3_bucket_maker(s3_connection):

    def maker(bucket_name):
        return s3_connection.create_bucket(bucket_name)

    return maker


@pytest.fixture
def s3_file_maker(s3_bucket_maker):

    def maker(bucket, key, content, metadata):
        b = s3_bucket_maker(bucket)
        k = Key(b)
        k.key = key
        if metadata:
            k.set_metadata('datalake', json.dumps(metadata))
        k.set_contents_from_string(content)

    return maker


@pytest.fixture
def s3_file_from_record(s3_file_maker):

    def maker(url, metadata):
        url = urlparse(url)
        assert url.scheme == 's3'
        s3_file_maker(url.netloc, url.path, '', metadata)

    return maker

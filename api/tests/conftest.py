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

import pytest

from moto import mock_dynamodb2
import boto3
import botocore.exceptions

from datalake_api import app as datalake_api
from datalake.tests import *  # noqa
from datalake.common import DatalakeRecord


YEAR_2010 = 1262304000000


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


def client_func():
    datalake_api.app.config['TESTING'] = True
    datalake_api.app.config['AWS_ACCESS_KEY_ID'] = 'abc'
    datalake_api.app.config['AWS_SECRET_ACCESS_KEY'] = '123'
    return datalake_api.app.test_client()

@pytest.fixture
def client():
    return client_func()

@pytest.fixture
def dynamodb(request):
    mock = mock_dynamodb2()
    mock.start()

    def tear_down():
        mock.stop()
    request.addfinalizer(tear_down)

    return boto3.resource('dynamodb',
                          region_name='us-west-2',
                          aws_secret_access_key='123',
                          aws_access_key_id='abc')


attribute_definitions = [
    {
        'AttributeName': 'time_index_key',
        'AttributeType': 'S'
    },
    {
        'AttributeName': 'work_id_index_key',
        'AttributeType': 'S'
    },
    {
        'AttributeName': 'range_key',
        'AttributeType': 'S'
    }
]

key_schema = [
    {
        'AttributeName': 'time_index_key',
        'KeyType': 'HASH'
    },
    {
        'AttributeName': 'range_key',
        'KeyType': 'RANGE'
    }
]

global_secondary = [{
    'IndexName': 'work-id-index',
    'KeySchema': [
        {
            'AttributeName': 'work_id_index_key',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'range_key',
            'KeyType': 'RANGE'
        }
    ],
    'Projection': {
        'ProjectionType': 'ALL'
    },
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5,
    }
}]


def _delete_table(table):
    try:
        table.delete()
    except botocore.exceptions.ClientError as e:
        stat = e.response.get('ResponseMetadata').get('HTTPStatusCode')
        code = e.response.get('Error').get('Code')
        if stat == 400 and code == 'ResourceNotFoundException':
            return
        raise e


def _create_table(dynamodb, table_name):
    table = dynamodb.Table(table_name)
    _delete_table(table)
    kwargs = dict(
        TableName=table_name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        GlobalSecondaryIndexes=global_secondary,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    dynamodb.create_table(**kwargs)
    return dynamodb.Table(table_name)


def _populate_table(table, records):
    with table.batch_writer() as batch:
        for r in records:
            batch.put_item(Item=r)


@pytest.fixture
def table_maker(request, dynamodb):

    def maker(records):
        table_name = 'test'
        table = _create_table(dynamodb, table_name)
        _populate_table(table, records)

        def tear_down():
            _delete_table(table)
        request.addfinalizer(tear_down)

        return table

    return maker


@pytest.fixture
def record_maker(s3_file_from_metadata):

    def maker(**kwargs):
        m = random_metadata_func()
        m.update(**kwargs)
        key = '/'.join([str(v) for v in kwargs.values()])
        url = 's3://datalake-test/' + key
        s3_file_from_metadata(url, m)
        return DatalakeRecord.list_from_metadata(url, m)

    return maker

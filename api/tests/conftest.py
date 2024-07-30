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
import boto3
from botocore.exceptions import (
    ClientError as BotoClientError,
    NoCredentialsError
)
from moto import mock_dynamodb2

from datalake_api import app as datalake_api
from datalake.tests import *  # noqa
from datalake.common import DatalakeRecord
from datalake.tests import generate_random_metadata


YEAR_2010 = 1262304000000


# If we run with proper AWS credentials they will be used
# This will cause moto to fail
# But more critically, may impact production systems
# So we test for real credentials and fail hard if they exist
sts = boto3.client('sts')
try:
    sts.get_caller_identity()
    pytest.exit("Real AWS credentials detected, aborting", 3)
except NoCredentialsError:
    pass  # no credentials are good


def get_client():
    from datalake_api import settings
    datalake_api.app.config.from_object(settings)

    datalake_api.app.config['TESTING'] = True
    datalake_api.app.config['AWS_ACCESS_KEY_ID'] = 'abc'
    datalake_api.app.config['AWS_SECRET_ACCESS_KEY'] = '123'

    # TODO: Sigh. The api caches the archive_fetcher and s3_bucket, which is
    # the right thing. However, because moto<3 still uses httpretty, and
    # because httpretty wreaks havoc on the python socket code, these cached
    # parts end up in a bad state after their first use. The right thing to do
    # here is to upgrade moto. But for that we will also have to move
    # everything from boto to boto3. This is a near-term goal. But first lets
    # get everything off of python2.
    for a in ('archive_fetcher', 's3_bucket'):
        try:
            delattr(datalake_api.app, a)
        except AttributeError:
            pass
    return datalake_api.app.test_client()


@pytest.fixture(scope='function')
def client():
    return get_client()


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

latest_attribute_definitions = [
    {
        'AttributeName': 'what_where_key',
        'AttributeType': 'S'
    }
]

latest_key_schema = [
    {
        'AttributeName': 'what_where_key',
        'KeyType': 'HASH'
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
    except BotoClientError as e:
        stat = e.response.get('ResponseMetadata').get('HTTPStatusCode')
        code = e.response.get('Error').get('Code')
        if stat == 400 and code == 'ResourceNotFoundException':
            return
        raise e


def _create_table(dynamodb,
                  table_name,
                  attribute_definitions,
                  key_schema, 
                  global_secondary=None):
    table = dynamodb.Table(table_name)
    _delete_table(table)
    kwargs = dict(
        TableName=table_name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    if global_secondary:
        kwargs['GlobalSecondaryIndexes'] = global_secondary
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
        latest_table_name = 'test_latest'

        table = _create_table(dynamodb, table_name, attribute_definitions, key_schema, global_secondary)
        latest_table = _create_table(dynamodb, latest_table_name, latest_attribute_definitions, latest_key_schema)

        _populate_table(latest_table, records)
        _populate_table(table, records)

        def tear_down():
            _delete_table(table)
            _delete_table(latest_table)

        request.addfinalizer(tear_down)
        return (table, latest_table)

    return maker


@pytest.fixture
def record_maker(s3_file_from_metadata):

    def maker(**kwargs):
        m = generate_random_metadata()
        m.update(**kwargs)
        key = '/'.join([str(v) for v in kwargs.values()])
        url = 's3://datalake-test/' + key
        s3_file_from_metadata(url, m)
        records = DatalakeRecord.list_from_metadata(url, m)

        what = kwargs.get('what')
        where = kwargs.get('where')
        for record in records:
            record['what_where_key'] = f"{what}:{where}"

        return records

    return maker

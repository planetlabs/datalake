import pytest
import boto3
from botocore.exceptions import ClientError as BotoClientError

from datalake_api import app as datalake_api
from datalake_common.tests import *

@pytest.fixture
def client():
    datalake_api.app.config['TESTING'] = True
    return datalake_api.app.test_client()


@pytest.fixture
def dynamodb():
    return boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
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
    except BotoClientError as e:
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
    return dynamodb.create_table(**kwargs)


def _populate_table(table, records):
    for r in records:
        table.put_item(Item=r)


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

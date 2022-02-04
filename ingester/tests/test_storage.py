import os
from unittest import mock
from datalake_ingester import DynamoDBStorage


@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_dynamodb_store(dynamodb_users_table):
    storage = DynamoDBStorage('users')
    expected_user = {'name': 'John', 'last_name': 'Muir'}
    storage.store(expected_user)
    print("DD", dynamodb_users_table.get_item)
    user = dynamodb_users_table.get_item(Key={'name':'John', 'last_name':'Muir'})['Item']
    assert user == expected_user

@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_store_duplicate(dynamodb_users_table):
    storage = DynamoDBStorage('users')
    expected_user = {'name': 'Vanilla', 'last_name': 'Ice'}
    storage.store(expected_user)
    storage.store(expected_user)
    user = dynamodb_users_table.get_item(Key={'name':'Vanilla', 'last_name':'Ice'})['Item']
    assert user == expected_user

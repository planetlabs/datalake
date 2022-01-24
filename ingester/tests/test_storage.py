from moto import mock_dynamodb2
from datalake_ingester import DynamoDBStorage


@mock_dynamodb2
def test_dynamodb_store(dynamodb_users_table):
    storage = DynamoDBStorage('users')
    expected_user = {'name': 'John', 'last_name': 'Muir'}
    storage.store(expected_user)
    user = dict(dynamodb_users_table.get_item(Key={'name':'John', 'last_name':'Muir'}))
    assert dict(user) == expected_user


@mock_dynamodb2
def test_store_duplicate(dynamodb_users_table):
    storage = DynamoDBStorage('users')
    expected_user = {'name': 'Vanilla', 'last_name': 'Ice'}
    storage.store(expected_user)
    storage.store(expected_user)
    user = dict(dynamodb_users_table.get_item(Key={'name':'Vanilla', 'last_name':'Ice'}))
    assert dict(user) == expected_user

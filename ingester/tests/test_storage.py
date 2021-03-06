from datalake_ingester import DynamoDBStorage


def test_dynamodb_store(dynamodb_users_table, dynamodb_connection):
    storage = DynamoDBStorage('users', connection=dynamodb_connection)
    expected_user = {'name': 'John', 'last_name': 'Muir'}
    storage.store(expected_user)
    user = dict(dynamodb_users_table.get_item(name='John', last_name='Muir'))
    assert dict(user) == expected_user


def test_store_duplicate(dynamodb_users_table, dynamodb_connection):
    storage = DynamoDBStorage('users', connection=dynamodb_connection)
    expected_user = {'name': 'Vanilla', 'last_name': 'Ice'}
    storage.store(expected_user)
    storage.store(expected_user)
    user = dict(dynamodb_users_table.get_item(name='Vanilla', last_name='Ice'))
    assert dict(user) == expected_user

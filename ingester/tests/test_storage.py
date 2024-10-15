from datalake_ingester import DynamoDBStorage
from decimal import Decimal


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

def test_insert_new_record(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage(connection=dynamodb_connection)
    storage.latest_table_name = 'latest'

    new_record = {
        'what_where_key': 'syslog:ground_server2',
        'time_index_key': '15225:newlog',
        'range_key': 'new_server:12345abcde',
        'metadata': {
            'version': 1,
            'start': 1500000000000,
            'end': 1500000000010,
            'path': '/var/log/syslog.2',
            'work_id': None,
            'where': 'ground_server2',
            'what': 'syslog',
            'id': '34fb2d1ec54245c7a57e29ed5a6ea9b2',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://newfile/url',
        'create_time': 1500000000000
    }

    storage.store_latest(new_record)

    stored_record = dynamodb_latest_table.get_item(
        what_where_key=new_record['what_where_key']
    )
    assert stored_record['metadata']['start'] == new_record['metadata']['start']


def provide_test_records():
    file1 = {
        'what_where_key': 'syslog:ground_server2',
        'time_index_key': '15219:zlcdzvawsp',
        'range_key': 'lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9',
        'metadata': {
            'version': 1,
            'start': 1314877177402,
            'end': 1314877177412, # ends ten seconds later
            'path': '/var/log/syslog.2',
            'work_id': 'abc-123',
            'where': 'ground_server2',
            'what': 'syslog',
            'id': 'file1',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://existingfile/url',
        'create_time': 1314877177402
    }

    file2 = {
        'what_where_key': 'syslog:ground_server2',
        'time_index_key': '15219:zlcdzvawsp',
        'range_key': 'lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9',
        'metadata': {
            'version': 1,
            'start': 1314877177413,  # One millisecond later
            'end': 1314877177423, # ends ten seconds later
            'path': '/var/log/syslog.2',
            'work_id': 'abc-123',
            'where': 'ground_server2',
            'what': 'syslog',
            'id': 'file2',
            'hash': 'c5g3d8de24af342643d5b78a8f2b9b88'
            
        },
        'url': 's3://existingfile/url',
        'create_time': 1314877177403
    }

    file3 = {
        'what_where_key': 'syslog:s114',
        'time_index_key': '15220:syslog',
        'range_key': 'ground_server2:34fb2d1ec54245c7a57e29ed5a6ea9b2',
        'metadata': {
            'version': 1,
            'start': 1414877177402,
            'end': 1415128740728,
            'path': '/var/log/syslog.2',
            'work_id': 'foo-bizz',
            'where': 's114',
            'what': 'syslog',
            'id': 'file3',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://datalake/path_to_file1',
        'create_time': 1414877177402,
        'size': 1048576
    }

    return (file1, file2, file3)


def test_store_conditional_put_latest_multiple_files(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage(connection=dynamodb_connection)
    storage.latest_table_name = 'latest'
    file1, file2, file3 = provide_test_records()

    storage.store_latest(file3)
    storage.store_latest(file1)
    storage.store_latest(file2) # same what:where, but should replace file1 b/c newer
    
    query_what_where = 'syslog:ground_server2'

    records = [dict(i) for i in dynamodb_latest_table.scan()]

    res = dict(dynamodb_latest_table.get_item(what_where_key=query_what_where))
    assert res['metadata']['start'] == Decimal('1314877177413')
    assert len(records) == 2
    assert file2 == res


def test_store_conditional_put_newest_first(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage(connection=dynamodb_connection)
    storage.latest_table_name = 'latest'
    file1, file2, file3 = provide_test_records()

    storage.store_latest(file3)
    storage.store_latest(file2)
    storage.store_latest(file1)

    query_what_where = 'syslog:ground_server2'

    res = dict(dynamodb_latest_table.get_item(what_where_key=query_what_where))
    assert res['metadata']['id'] != file1['metadata']['id']
    assert res['metadata']['id'] == file2['metadata']['id']


def test_verify_replace_record_same_start(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage(connection=dynamodb_connection)
    storage.latest_table_name = 'latest'

    record1 = {
        'what_where_key': 'syslog:ground_server2',
        'time_index_key': '15225:newlog',
        'range_key': 'new_server:12345abcde',
        'metadata': {
            'version': 1,
            'start': 1500000000000,
            'end': 1500000000010,
            'path': '/var/log/syslog.2',
            'work_id': None,
            'where': 'ground_server2',
            'what': 'syslog',
            'id': '34fb2d1ec54245c7a57e29ed5a6ea9b2',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://newfile/url',
        'create_time': 1500000000000
    }

    record2 = {
        'what_where_key': 'syslog:ground_server2',
        'time_index_key': '15225:newlog',
        'range_key': 'new_server:12345abcde',
        'metadata': {
            'version': 1,
            'start': 1500000000000,
            'end': 1500000000010,
            'path': '/var/log/syslog.2',
            'work_id': None,
            'where': 'ground_server2',
            'what': 'syslog',
            'id': 'abc123',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://newfile/url',
        'create_time': 1500000000000
    }

    storage.store_latest(record1)
    storage.store_latest(record2)

    stored_record = dynamodb_latest_table.get_item(
        what_where_key="syslog:ground_server2"
    )
    assert stored_record['metadata']['start'] == record1['metadata']['start']
    assert stored_record['metadata']['id'] == "abc123"

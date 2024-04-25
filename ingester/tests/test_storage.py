from datalake_ingester import DynamoDBStorage
from decimal import Decimal
import boto3

def test_insert_new_record(dynamodb_latest_table, dynamodb_connection):
    boto3.setup_default_session(fake_credentials=True)
    storage = DynamoDBStorage('latest', connection=dynamodb_connection)
    new_record = {
        'time_index_key': '15225:newlog',
        'range_key': 'new_server:12345abcde',
        'metadata': {
            'start': 1500000000000
        },
        'url': 's3://newfile/url',
        'create_time': 1500000000000
    }

    storage.store_latest(new_record)

    stored_record = dynamodb_latest_table.get_item(
        time_index_key='15225:newlog',
        range_key='new_server:12345abcde'
    )
    assert stored_record['metadata']['start'] == new_record['metadata']['start']


def test_store_conditional_put_latest_multiple_files(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage('latest', connection=dynamodb_connection)

    file1 = {
        'time_index_key': '15219:zlcdzvawsp',
        'range_key': 'lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9',
        'metadata': {
            'start': 1314877177402
        },
        'url': 's3://existingfile/url',
        'create_time': 1314877177402
    }

    file2 = {
        'time_index_key': '15219:zlcdzvawsp',
        'range_key': 'lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9',
        'metadata': {
            'start': 1314877177403  # One millisecond later
        },
        'url': 's3://existingfile/url',
        'create_time': 1314877177403
    }

    file3 = {
        'time_index_key': '15220:syslog',
        'range_key': 'ground_server2:34fb2d1ec54245c7a57e29ed5a6ea9b2',
        'metadata': {
            'version': 1,
            'start': 1414877177402,
            'end': 1415128740728,
            'path': '/var/log/syslog.2',
            'work_id': None,
            'where': 'ground_server2',
            'what': 'syslog',
            'id': '34fb2d1ec54245c7a57e29ed5a6ea9b2',
            'hash': 'b4f2d8de24af342643d5b78a8f2b9b88'
        },
        'url': 's3://datalake/path_to_file1',
        'create_time': 1414877177402,
        'size': 1048576
    }

    storage.store_latest(file3)
    storage.store_latest(file1)
    storage.store_latest(file2) # same what:where, but should replace file1 b/c newer
    
    records = [dict(i) for i in dynamodb_latest_table.scan()]

    res = dict(dynamodb_latest_table.get_item(time_index_key='15219:zlcdzvawsp',
                                              range_key='lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9'))
    assert res['metadata']['start'] == Decimal('1314877177403')
    assert len(records) == 2
    assert file2 == res


def test_concurrent_updates(dynamodb_latest_table, dynamodb_connection):
    storage = DynamoDBStorage('latest', connection=dynamodb_connection)

    base_record = {
        'time_index_key': '15219:zlcdzvawsp',
        'range_key': 'lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9',
        'metadata': {
            'start': 1314877177402
        },
        'url': 's3://existingfile/url',
        'create_time': 1314877177402
    }
    storage.store(base_record)

    
    updated_record1 = base_record.copy()
    updated_record1['metadata']['start'] += 10

    updated_record2 = base_record.copy()
    updated_record2['metadata']['start'] += 5


    storage.store_latest(updated_record1)
    storage.store_latest(updated_record2)

    stored_record = dynamodb_latest_table.get_item(
        time_index_key='15219:zlcdzvawsp',
        range_key='lawvuunyws:447a4a801cabc6089f04922abdfa8aad099824e9'
    )

    assert stored_record['metadata']['start'] == updated_record1['metadata']['start']


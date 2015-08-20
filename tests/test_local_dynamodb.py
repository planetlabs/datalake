'''test local dynamodb install

Just a basic test to validate the dev environment.
'''
import pytest

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError


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
def dynamodb_users_table(dynamodb_connection):

    _delete_table_if_exists(dynamodb_connection, 'users')

    schema = [HashKey('name'), RangeKey('last_name')]
    throughput = {'read': 5, 'write': 5}
    table = Table.create('users',
                         schema=schema,
                         throughput=throughput,
                         connection=dynamodb_connection)
    return table


def test_list_table(dynamodb_users_table, dynamodb_connection):
    table_list = dynamodb_connection.list_tables()
    assert 'TableNames' in table_list
    table_list = table_list['TableNames']
    assert len(table_list) == 1
    assert table_list[0] == 'users'

import pytest

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from boto.dynamodb2.fields import HashKey, RangeKey


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


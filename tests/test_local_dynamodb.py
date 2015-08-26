'''test local dynamodb install

Just a basic test to validate the dev environment.
'''
import pytest


def test_list_table(dynamodb_users_table, dynamodb_connection):
    table_list = dynamodb_connection.list_tables()
    assert 'TableNames' in table_list
    table_list = table_list['TableNames']
    assert len(table_list) == 1
    assert table_list[0] == 'users'

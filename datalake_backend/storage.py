from memoized_property import memoized_property
import boto.dynamodb2
from boto.dynamodb2.table import Table

from conf import get_config_var
from errors import InsufficientConfiguration


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name, connection=None):
        self.table_name = table_name
        self._prepare_connection(connection)

    @classmethod
    def from_config(cls):
        table_name = get_config_var('dynamodb_table')
        if table_name is None:
            raise InsufficientConfiguration('Please specify a dynamodb table')
        return cls(table_name)

    def _prepare_connection(self, connection):
        region = get_config_var('aws_region')
        if connection:
            self._connection = connection
        elif region:
            self._connection = boto.dynamodb2.connect_to_region(region)
        else:
            msg = 'Please provide a connection or configure a region'
            raise InsufficientConfiguration(msg)

    @memoized_property
    def _table(self):
        return Table(self.table_name, connection=self._connection)

    def store(self, record):
        self._table.put_item(data=record)

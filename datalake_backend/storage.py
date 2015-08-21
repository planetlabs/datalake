from memoized_property import memoized_property
from boto.dynamodb2.table import Table

from conf import get_config
from errors import InsufficientConfiguration


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name=None, connection=None):
        self._set_table_name(table_name)
        self._prepare_connection(connection)

    def _set_table_name(self, table_name):
        self.table_name = table_name or get_config().dynamodb_table
        if self.table_name is None:
            raise InsufficientConfiguration('Please specify a dynamodb table')

    def _prepare_connection(self, connection):
        region = get_config().aws_region
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

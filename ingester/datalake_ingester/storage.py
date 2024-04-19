# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from memoized_property import memoized_property
import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import (ConditionalCheckFailedException,
                                       ItemNotFound)
import os
from datalake.common.errors import InsufficientConfiguration


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name, connection=None):
        self.table_name = table_name
        self._prepare_connection(connection)

    @classmethod
    def from_config(cls):
        table_name = os.environ.get('DATALAKE_DYNAMODB_TABLE')
        if table_name is None:
            raise InsufficientConfiguration('Please specify a dynamodb table')
        return cls(table_name)

    def _prepare_connection(self, connection):
        region = os.environ.get('AWS_REGION')
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
        try:
            primary_key = {
                'time_index_key': record['time_index_key'],
                'range_key': record['range_key']
            }
            existing_record = self._table.get_item(**primary_key)

            if existing_record and \
                existing_record['metadata']['start'] < record['metadata']['start']:
                 self._table.put_item(data=record, overwrite=True)
            else:
                print(f'Existing record in latest table has later or same start time. No update performed')

        except ItemNotFound:
            # Item doesn't exist, lets insert
            self._table.put_item(data=record)
        except ConditionalCheckFailedException:
                # Tolerate duplicate stores
                pass

    def update(self, record):
        self._table.put_item(data=record, overwrite=True)

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
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
import os
from datalake.common.errors import InsufficientConfiguration
import logging


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name=None, latest_table=None, connection=None):
        self.table_name = table_name
        self.latest_table_name = os.environ.get("DATALAKE_LATEST_TABLE",
                                                f"{latest_table}")
        self.use_latest = os.environ.get("DATALAKE_USE_LATEST_TABLE", False)
        self._prepare_connection(connection)
        self.logger = logging.getLogger('storage')

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
    
    @memoized_property
    def _latest_table(self):
        return Table(self.latest_table_name, connection=self._connection)

    def store(self, record):
        if self.use_latest:
            print(f'use_latest is {self.use_latest}')
            self.store_latest(record)
        else:
            try:
                self._table.put_item(data=record)
            except ConditionalCheckFailedException:
                # Tolerate duplicate stores
                pass

    def update(self, record):
        self._table.put_item(data=record, overwrite=True)

    def store_latest(self, record):
        """
        note: Record must utilize AttributeValue syntax
              for the conditional put.
        """

        condition_expression = " attribute_not_exists(what_where_key) OR metadata.start < :new_start"
        expression_attribute_values = {
            ':new_start': {'N': str(record['metadata']['start'])}
        }
        record = {
            'what_where_key': {"S": record['metadata']['what']+':'+record['metadata']['where']},
            'time_index_key': {"S": record['time_index_key']},
            'range_key': {"S": record['range_key']},
            'metadata': {
                'M': {
                    'start': {
                        'N': str(record['metadata']['start'])
                    },
                    'end': {
                        'N': str(record['metadata']['end'])
                    },
                    'id': {
                        'S': str(record['metadata']['id'])
                    },
                    'path': {
                        'S': str(record['metadata']['path'])
                    },
                    'hash': {
                        'S': str(record['metadata']['hash'])
                    },
                    'version': {
                        'N': str(record['metadata']['version'])
                    },
                    'what': {
                        'S': str(record['metadata']['what'])
                    },
                    'where': {
                        'S': str(record['metadata']['where'])
                    },
                    'work_id': {
                        'S': str(record['metadata']['work_id'])
                    }
                }
            },
            'url': {"S": record['url']},
            'create_time': {'N': str(record['create_time'])}
        }

        try:
            self._connection.put_item(
                table_name=self.latest_table_name,
                item=record,
                condition_expression=condition_expression,
                expression_attribute_values=expression_attribute_values
            )
            self.logger.info("Record stored successfully.")
        except ConditionalCheckFailedException:
            self.logger.error("Condition not met, no operation was performed.")
        except Exception as e:
            self.logger.error(f"Error occurred: {str(e)}")


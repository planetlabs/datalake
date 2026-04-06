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
import boto3
from botocore.exceptions import ClientError
import os
from datalake.common.errors import InsufficientConfiguration
import logging


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name=None, latest_table_name=None, connection=None):
        self.table_name = table_name
        self.latest_table_name = latest_table_name
        self.logger = logging.getLogger('storage')
        self._prepare_connection(connection)


    @classmethod
    def from_config(cls):
        table_name = os.environ.get('DATALAKE_DYNAMODB_TABLE')
        latest_table_name = os.environ.get("DATALAKE_LATEST_TABLE")
        if table_name is None:
            raise InsufficientConfiguration('Please specify a dynamodb table')
        return cls(table_name, latest_table_name)

    def _prepare_connection(self, connection):
        self.logger.info("Preparing connection...")
        region = os.environ.get('AWS_REGION')
        if connection:
            self._connection = connection
            self._client = connection.meta.client
        elif region:
            self._connection = boto3.resource('dynamodb', region_name=region)
            self._client = self._connection.meta.client

        else:
            msg = 'Please provide a connection or configure a region'
            raise InsufficientConfiguration(msg)

    @memoized_property
    def _table(self):
        return self._connection.Table(self.table_name)

    @memoized_property
    def _latest_table(self):
        return self._connection.Table(self.latest_table_name)

    def store(self, record):
        try:
            self._table.put_item(Item=record)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                pass
            else:
                raise
        if self.latest_table_name:
            self.store_latest(record)

    def update(self, record):
        self._table.put_item(Item=record)

    def store_latest(self, record):
        """
        Store the latest record for a given what:where key with conditional put.
        """
        condition_expression = "attribute_not_exists(what_where_key) OR metadata.#metadata_start <= :new_start"

        expression_attribute_values = {
            ':new_start': record['metadata']['start']
        }

        expression_attribute_names = {
            '#metadata_start': "start"
        }


        if record['metadata']['work_id'] is None:
            work_id_value = None
        else:
            work_id_value = str(record['metadata']['work_id'])

        if record['metadata']['end'] is None:
            end_time_value = None
        else:
            end_time_value = record['metadata']['end']

        record = {
            'what_where_key': record['metadata']['what']+':'+record['metadata']['where'],
            'time_index_key': record['time_index_key'],
            'range_key': record['range_key'],
            'metadata': {
                'start': record['metadata']['start'],
                'end': end_time_value,
                'id': str(record['metadata']['id']),
                'path': str(record['metadata']['path']),
                'hash': str(record['metadata']['hash']),
                'version': record['metadata']['version'],
                'what': str(record['metadata']['what']),
                'where': str(record['metadata']['where']),
                'work_id': work_id_value
            },
            'url': record['url'],
            'create_time': record['create_time']
        }
        self.logger.info(f"Attempting to store record: {record}")
        try:
            self._latest_table.put_item(
                Item=record,
                ConditionExpression=condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
            self.logger.info("Record stored successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.logger.debug(f"Condition not met for record {record},"
                              "no operation was performed.")
            else:
                raise
        except Exception as e:
            self.logger.error(f"Error occurred while attempting {record}: {str(e)}")

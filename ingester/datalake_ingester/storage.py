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
import os
import boto3
from datalake.common.errors import InsufficientConfiguration


class DynamoDBStorage(object):
    '''store datalake records in a dynamoDB table'''

    def __init__(self, table_name):
        self.table_name = table_name
        self._prepare_connection(connection)

    @classmethod
    def from_config(cls):
        table_name = os.environ.get('DATALAKE_DYNAMODB_TABLE')
        if table_name is None:
            raise InsufficientConfiguration('Please specify a dynamodb table')
        return cls(table_name)

    @memoized_property
    def _dynamodb(self):
        region = os.environ.get('AWS_REGION')
        if region:
            return boto3.resource('dynamodb', region_name=region)
        else:
            return boto3.resource('dynamodb')

    @memoized_property
    def _table(self):
        return self._dynamodb.Table(self.table_name)

    def store(self, record):
        # Will overwrite item if it exists
        self._table.put_item(Item=record)

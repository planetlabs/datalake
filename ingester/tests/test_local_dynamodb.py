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

'''test local dynamodb install

Just a basic test to validate the dev environment.
'''


def test_list_table(dynamodb_users_table, dynamodb_connection):
    table_list = dynamodb_connection.list_tables()
    assert 'TableNames' in table_list
    table_list = table_list['TableNames']
    assert len(table_list) == 1
    assert table_list[0] == 'users'

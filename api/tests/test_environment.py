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
import simplejson as json


def test_environment(client):
    uri = '/v0/environment/'
    res = client.get(uri)
    assert res.status_code == 200
    response = json.loads(res.get_data())
    assert 'build' in response['data'] and \
        'version' in response['data']['build']

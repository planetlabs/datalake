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
import base64


def get_bad_request(client, params):
    uri = '/v0/archive/files/'
    q = '&'.join(['{}={}'.format(k, v) for k, v in params.iteritems()])
    if q:
        uri += '?' + q
    res = client.get(uri)
    assert res.status_code == 400
    response = json.loads(res.get_data())
    assert 'code' in response
    assert 'message' in response
    return response


def test_no_parameters(client):
    res = get_bad_request(client, {})
    assert res['code'] == 'NoArgs'


def test_no_what_parameter(client):
    res = get_bad_request(client, {'start': 123})
    assert res['code'] == 'NoWhat'


def test_no_work_id_or_interval(client):
    res = get_bad_request(client, {'what': 'syslog'})
    assert res['code'] == 'NoWorkInterval'


def test_work_id_and_start(client):
    params = {
        'what': 'syslog',
        'work_id': 'work123',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_work_id_and_end(client):
    params = {
        'what': 'syslog',
        'work_id': 'work123',
        'end': 345
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_start_without_end(client):
    params = {
        'what': 'syslog',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_end_without_start(client):
    params = {
        'what': 'syslog',
        'end': 345
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_invalid_start(client):
    params = {
        'what': 'syslog',
        'start': 'notaninteger',
        'end': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidTime'


def test_invalid_end(client):
    params = {
        'what': 'syslog',
        'end': 'notaninteger',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidTime'


def test_start_after_end(client):
    params = {
        'what': 'syslog',
        'end': 100,
        'start': 200,
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_invalid_cursor(client):
    params = {
        'what': 'syslog',
        'start': 100,
        'end': 200,
        'cursor': 'foobar',
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidCursor'


def test_bad_cursor_valid_json(client):
    cursor = base64.b64encode('{"valid": "json", "invalid": "cursor"}')
    params = {
        'what': 'syslog',
        'start': 100,
        'end': 200,
        'cursor': cursor,
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidCursor'

def test_bad_cursor_valid_json(client):
    cursor = base64.b64encode('{"valid": "json", "invalid": "cursor"}')
    params = {
        'what': 'syslog',
        'start': 100,
        'end': 200,
        'cursor': cursor,
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidCursor'

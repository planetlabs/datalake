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

def test_swagger_spec(client):
    res = client.get('/spec/')
    assert res.status_code == 200


def assert_redirect(res, location):
    assert res.status_code == 302
    assert 'Location' in res.headers
    assert res.headers['Location'].endswith(location)


def test_swagger_doc_redirect(client):
    res = client.get('/')
    assert_redirect(res, '/docs/')
    res = client.get('/docs/')
    assert_redirect(res, '/static/index.html')

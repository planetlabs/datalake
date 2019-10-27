# Copyright 2016 Planet Labs, Inc.
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
import pytest
import simplejson as json


@pytest.fixture
def metadata_getter(client):

    def getter(file_id):
        uri = '/v0/archive/files/' + file_id + '/metadata'
        return client.get(uri)

    return getter


def test_get_metadata(metadata_getter, s3_file_maker, random_metadata):
    random_metadata['id'] = '12345'
    content = 'once upon a time'
    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = metadata_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'application/json'
    assert json.loads(res.data) == random_metadata


def test_no_such_metadata(s3_bucket_maker, metadata_getter):
    s3_bucket_maker('datalake-test')
    res = metadata_getter('12345')
    assert res.status_code == 404
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'NoSuchFile'
    assert 'message' in response

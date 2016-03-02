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
import pytest
import gzip
from StringIO import StringIO
import simplejson as json


@pytest.fixture
def file_getter(client):

    def getter(file_id):
        uri = '/v0/archive/files/' + file_id + '/data'
        return client.get(uri)

    return getter


def test_get_text_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.txt'
    random_metadata['id'] = '12345'
    content = 'once upon a time'
    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding is None
    assert res.get_data() == 'once upon a time'


def create_gzip_string(content):
    fgz = StringIO()
    gzip_obj = gzip.GzipFile(mode='wb', fileobj=fgz)
    gzip_obj.write(content)
    gzip_obj.close()
    fgz.seek(0)
    return fgz.read()


def test_get_gzipped_text_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.txt.gz'
    random_metadata['id'] = '12345'
    content = create_gzip_string('no place like home')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding == 'gzip'
    assert res.get_data() == content


def test_get_json_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.json'
    random_metadata['id'] = '12345'
    content = '{"this": "is json"}'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'application/json'
    assert res.content_encoding is None
    assert res.get_data() == content


def test_get_syslog(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'syslog'
    random_metadata['path'] = '/var/log/syslog.1'
    random_metadata['id'] = '12345'
    content = 'boot\nrun application\ncrash\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding is None
    assert res.get_data() == content


def test_get_random_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log'
    random_metadata['id'] = '12345'
    content = '42\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding is None
    assert res.get_data() == content


def test_get_random_rotated_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log.1'
    random_metadata['id'] = '12345'
    content = '93\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding is None
    assert res.get_data() == content


def test_get_random_gz_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log.gz'
    random_metadata['id'] = '12345'
    content = create_gzip_string('431\n')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding == 'gzip'
    assert res.get_data() == content


def test_get_log_trailing_sha1(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'foolog'
    random_metadata['path'] = '/var/log/foo/foo.log.gz-e6294fa5eddbc9d38bd7a20f072ffd3a182fa1e7'  # noqa
    random_metadata['id'] = '12345'
    content = create_gzip_string('welcome to foo land')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding == 'gzip'
    assert res.get_data() == content


def test_get_log_trailing_md5(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'barlog'
    random_metadata['path'] = '/var/log/bar/bar.log-e6294fa5eddbc9d38bd7a20f072ffd3a'  # noqa
    random_metadata['id'] = '12345'
    content = 'welcome to bar land'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    assert res.status_code == 200
    assert res.content_type == 'text/plain'
    assert res.content_encoding is None
    assert res.get_data() == content


def test_no_such_id(s3_bucket_maker, file_getter):
    s3_bucket_maker('datalake-test')
    res = file_getter('12345')
    assert res.status_code == 404
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'NoSuchFile'
    assert 'message' in response

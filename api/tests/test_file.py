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
import io
import simplejson as json
from datalake_api.fetcher import ArchiveFile
import time
from urllib.parse import urlencode
from datalake.common import DatalakeRecord


@pytest.fixture
def file_getter(client):

    def getter(file_id):
        uri = '/v0/archive/files/' + file_id + '/data'
        return client.get(uri)

    return getter


def _validate_file_result(result, content, content_type='text/plain',
                          content_encoding=None):
    assert result.status_code == 200
    assert result.content_type == content_type
    assert result.content_encoding == content_encoding
    assert result.get_data() == content


def test_get_text_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.txt'
    random_metadata['id'] = '12345'
    content = b'once upon a time'
    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content)


def create_gzip_string(content):
    fgz = io.BytesIO()
    gzip_obj = gzip.GzipFile(mode='wb', fileobj=fgz)
    gzip_obj.write(content)
    gzip_obj.close()
    fgz.seek(0)
    return fgz.read()


def test_get_gzipped_text_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.txt.gz'
    random_metadata['id'] = '12345'
    content = create_gzip_string(b'no place like home')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content, content_encoding='gzip')


def test_get_json_file(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/home/you/foo.json'
    random_metadata['id'] = '12345'
    content = b'{"this": "is json"}'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content, content_type='application/json')


def test_get_syslog(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'syslog'
    random_metadata['path'] = '/var/log/syslog.1'
    random_metadata['id'] = '12345'
    content = b'boot\nrun application\ncrash\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content)


def test_get_random_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log'
    random_metadata['id'] = '12345'
    content = b'42\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content)


def test_get_random_rotated_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log.1'
    random_metadata['id'] = '12345'
    content = b'93\n'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content)


def test_get_random_gz_log(file_getter, s3_file_maker, random_metadata):
    random_metadata['path'] = '/var/log/random.log.gz'
    random_metadata['id'] = '12345'
    content = create_gzip_string(b'431\n')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content, content_encoding='gzip')


def test_get_log_trailing_sha1(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'foolog'
    random_metadata['path'] = '/var/log/foo/foo.log.gz-e6294fa5eddbc9d38bd7a20f072ffd3a182fa1e7'  # noqa
    random_metadata['id'] = '12345'
    content = create_gzip_string(b'welcome to foo land')

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content, content_encoding='gzip')


def test_get_log_trailing_md5(file_getter, s3_file_maker, random_metadata):
    random_metadata['what'] = 'barlog'
    random_metadata['path'] = '/var/log/bar/bar.log-e6294fa5eddbc9d38bd7a20f072ffd3a'  # noqa
    random_metadata['id'] = '12345'
    content = b'welcome to bar land'

    s3_file_maker('datalake-test', '12345/data', content, random_metadata)
    res = file_getter('12345')
    _validate_file_result(res, content)


def test_no_such_id(s3_bucket_maker, file_getter):
    s3_bucket_maker('datalake-test')
    res = file_getter('12345')
    assert res.status_code == 404
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'NoSuchFile'
    assert 'message' in response


def test_archive_file_read_twice(tmpfile, random_metadata):
    content = 'it was the best of times, it was the worst of times'
    f = open(tmpfile(content))
    af = ArchiveFile(f, random_metadata)
    assert af.read() == content
    assert af.read() == ''


def test_archive_file_bigger_than_header(tmpfile, random_metadata):
    content = 'x' * 1024 + 'y' * 1024
    f = open(tmpfile(content))
    af = ArchiveFile(f, random_metadata)
    assert af.read() == content
    assert af.read() == ''


@pytest.fixture
def latest_getter(client):

    def getter(what, where, **kwargs):
        uri = '/v0/archive/latest/{}/{}/data'.format(what, where)
        if kwargs:
            uri += '?' + urlencode(kwargs)
        return client.get(uri)

    return getter


@pytest.fixture
def record_maker(table_maker, s3_file_maker):

    def maker(content, metadata):
        path = metadata['id'] + '/data'
        s3_file_maker('datalake-test', path, content, metadata)
        url = 's3://datalake-test/' + path
        records = DatalakeRecord.list_from_metadata(url, metadata)
        table_maker(records)

    return maker


def test_get_latest_text_file(record_maker, latest_getter, random_metadata):
    now = int(time.time() * 1000)
    random_metadata['path'] = '/home/you/foo.txt'
    random_metadata['id'] = '12345'
    random_metadata['what'] = 'text'
    random_metadata['where'] = 'there'
    random_metadata['start'] = now
    random_metadata['end'] = None
    content = b'once upon a time'
    record_maker(content, random_metadata)
    res = latest_getter('text', 'there')
    _validate_file_result(res, content)


def test_latest_gzipped_text_file(record_maker, latest_getter,
                                  random_metadata):
    now = int(time.time() * 1000)
    random_metadata['path'] = '/home/you/foo.txt.gz'
    random_metadata['id'] = '12345'
    random_metadata['what'] = 'gz'
    random_metadata['where'] = 'here'
    random_metadata['start'] = now
    random_metadata['end'] = None
    content = create_gzip_string(b'no place like home')
    record_maker(content, random_metadata)
    res = latest_getter('gz', 'here')
    _validate_file_result(res, content, content_encoding='gzip')


def test_no_such_where_when(table_maker, latest_getter):
    table_maker([])
    res = latest_getter('this', 'that')
    assert res.status_code == 404
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'NoSuchFile'
    assert 'message' in response


def test_non_default_lookback(record_maker, random_metadata, latest_getter):
    MS_PER_DAY = 24 * 60 * 60 * 1000
    now = int(time.time() * 1000) - 20 * MS_PER_DAY
    random_metadata['what'] = 'text'
    random_metadata['where'] = 'there'
    random_metadata['start'] = now
    random_metadata['end'] = None
    content = b'once upon a time'
    record_maker(content, random_metadata)

    res = latest_getter('text', 'there', lookback=19)
    assert res.status_code == 404
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'NoSuchFile'
    assert 'message' in response

    res = latest_getter('text', 'there', lookback=20)
    _validate_file_result(res, content)


def test_invalid_lookback(record_maker, random_metadata, latest_getter):
    res = latest_getter('text', 'there', lookback='foo')
    assert res.status_code == 400
    response = json.loads(res.get_data())
    assert 'code' in response
    assert response['code'] == 'InvalidLookback'
    assert 'message' in response

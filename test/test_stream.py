# Copyright 2017 Planet Labs, Inc.
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
from datalake import InvalidDatalakePath
import responses
from io import BytesIO


def test_invalid_stream_url(archive):
    with pytest.raises(InvalidDatalakePath):
        archive.stream_contents('x4ti://foobar/bing')


def test_stream_url_without_key(archive):
    with pytest.raises(InvalidDatalakePath):
        archive.stream_contents(archive.storage_url)


def test_key_does_not_exist(archive):
    url = archive.storage_url + '/nosuchfileishere'
    with pytest.raises(InvalidDatalakePath):
        archive.stream_contents(url)


def test_invalid_url(archive, random_metadata):
    url = 'http://datalake.example.com/v0/archive/files/1234/'
    with pytest.raises(InvalidDatalakePath):
        archive.stream_contents(url)


def test_invalid_protocol(archive, random_metadata):
    url = 'ftp://alternate-datalake.example.com/v0/archive/files/1234/data'
    with pytest.raises(InvalidDatalakePath):
        archive.stream_contents(url)


def _stream_bytes_to_buffer(stream):
    f = BytesIO()
    for block in stream:
        f.write(block)
    f.seek(0)
    return f


def test_stream(archive, datalake_url_maker, random_metadata):
    content = 'welcome to the jungle'.encode('utf-8')
    url = datalake_url_maker(metadata=random_metadata,
                             content=content)
    s = archive.stream_contents(url)
    f = _stream_bytes_to_buffer(s)
    assert f.read() == content


@responses.activate
def test_stream_http_url(archive, random_metadata):
    base_url = 'http://datalake.example.com/v0/archive/files/1234/'
    content = 'foobar'.encode('utf-8')
    responses.add(responses.GET, base_url + 'data', body=content,
                  content_type='text/plain', status=200)
    responses.add(responses.GET, base_url + 'metadata', json=random_metadata,
                  content_type='application/json', status=200)
    s = archive.stream_contents(base_url + 'data')
    f = _stream_bytes_to_buffer(s)
    assert f.read() == content


@responses.activate
def test_stream_http_url_chunking(archive, random_metadata):
    base_url = 'http://datalake.example.com/v0/archive/files/1234/'
    content = ('0' * 4096).encode('utf-8')
    responses.add(responses.GET, base_url + 'data', body=content,
                  content_type='text/plain', status=200)
    responses.add(responses.GET, base_url + 'metadata', json=random_metadata,
                  content_type='application/json', status=200)
    stream = archive.stream_contents(base_url + 'data')

    chunk_lengths = []
    for chunk in stream:
        assert len(chunk) < len(content)
        chunk_lengths += [len(chunk)]
    assert sum(chunk_lengths) == len(content)

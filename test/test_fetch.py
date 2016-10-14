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
from datalake import InvalidDatalakePath
import os
import responses


def test_invalid_fetch_url(archive):
    with pytest.raises(InvalidDatalakePath):
        archive.fetch('x4t://foobar/bing')


def test_fetch_url_without_key(archive):
    with pytest.raises(InvalidDatalakePath):
        archive.fetch(archive.storage_url)


def test_key_does_not_exist(archive):
    url = archive.storage_url + '/nosuchfile'
    with pytest.raises(InvalidDatalakePath):
        archive.fetch(url)


def test_fetch(archive, datalake_url_maker, random_metadata):
    content = 'welcome to the jungle'.encode('utf-8')
    url = datalake_url_maker(metadata=random_metadata,
                             content=content)
    f = archive.fetch(url)
    assert f.read() == content


def test_fetch_to_file(monkeypatch, archive, datalake_url_maker,
                       random_metadata, tmpdir):
    monkeypatch.chdir(str(tmpdir))
    url = datalake_url_maker(metadata=random_metadata,
                             content='now with more jingle')
    archive.fetch_to_filename(url)
    assert os.path.exists(random_metadata['id'])
    contents = open(random_metadata['id']).read()
    assert contents == 'now with more jingle'


def test_fetch_to_fancy_template(archive, datalake_url_maker, random_metadata,
                                 tmpdir):
    url = datalake_url_maker(metadata=random_metadata)
    t = os.path.join(str(tmpdir), '{where}/{what}/{start}-{id}-foobar.log')
    fname = '{}-{}-foobar.log'
    fname = fname.format(random_metadata['start'], random_metadata['id'])
    expected_path = os.path.join(str(tmpdir), random_metadata['where'],
                                 random_metadata['what'], fname)
    archive.fetch_to_filename(url, filename_template=t)
    assert os.path.exists(expected_path)


def test_no_such_metadata_field_in_template(archive, datalake_url_maker):
    url = datalake_url_maker()
    with pytest.raises(InvalidDatalakePath):
        archive.fetch_to_filename(url, filename_template='{nosuchmeta}')


def test_bad_template(archive, datalake_url_maker):
    url = datalake_url_maker()
    with pytest.raises(InvalidDatalakePath):
        archive.fetch_to_filename(url, filename_template='{bad')


def test_cli_fetch_to_file(monkeypatch, cli_tester, datalake_url_maker,
                           random_metadata, tmpdir):
    monkeypatch.chdir(str(tmpdir))
    url = datalake_url_maker(metadata=random_metadata,
                             content='look ma, CLI')

    cmd = 'fetch ' + url
    output = cli_tester(cmd)

    assert output == random_metadata['id'] + '\n'
    assert os.path.exists(random_metadata['id'])
    contents = open(random_metadata['id']).read()
    assert contents == 'look ma, CLI'


@responses.activate
def test_fetch_http_url(archive, random_metadata):
    base_url = 'http://datalake.example.com/v0/archive/files/1234/'
    content = 'foobar'.encode('utf-8')
    responses.add(responses.GET, base_url + 'data', body=content,
                  content_type='text/plain', status=200)
    responses.add(responses.GET, base_url + 'metadata', json=random_metadata,
                  content_type='application/json', status=200)
    f = archive.fetch(base_url + 'data')
    assert f.metadata == random_metadata
    assert f.read() == content


def test_invalid_url(archive, random_metadata):
    url = 'http://datalake.example.com/v0/archive/files/1234/'
    with pytest.raises(InvalidDatalakePath):
        archive.fetch(url)


def test_invalid_protocol(archive, random_metadata):
    url = 'ftp://alternate-datalake.example.com/v0/archive/files/1234/data'
    with pytest.raises(InvalidDatalakePath):
        archive.fetch(url)


@responses.activate
def test_metadata_from_http_url(archive, random_metadata):
    url = 'http://datalake.example.com/v0/archive/files/1234data'
    content = 'foobody'.encode('utf-8')
    responses.add(responses.GET, url + '/data', body=content,
                  content_type='text/plain', status=200)
    responses.add(responses.GET, url + '/metadata', json=random_metadata,
                  content_type='application/json', status=200)
    f = archive.fetch(url + '/data')
    assert f.read() == content
    assert f.metadata == random_metadata

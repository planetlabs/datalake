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
from datalake_common.tests import random_word, random_metadata
from datalake_common import InvalidDatalakeMetadata
import os
import json
import tarfile
from cStringIO import StringIO

from datalake import File, InvalidDatalakeBundle


def random_file(tmpdir, metadata=None):
    name = random_word(10)
    content = random_word(256)
    f = tmpdir.join(name)
    f.write(content)
    if metadata is None:
        metadata = random_metadata()
    return File.from_filename(f.strpath, **metadata)


@pytest.fixture
def random_files(tmpdir):
    def get_randfiles(n):
        return [random_file(tmpdir) for _ in range(n)]
    return get_randfiles


def test_file_hash_different(random_files):
    files = random_files(2)
    assert files[0].metadata['hash'] != files[1].metadata['hash']


def test_non_existent_file():
    with pytest.raises(IOError):
        File.from_filename('surelythisfiledoesnotexist.txt')


def test_not_enough_metadata(tmpdir):
    with pytest.raises(InvalidDatalakeMetadata):
        random_file(tmpdir, metadata={'where': 'foo'})


def test_hash_not_overwritten(tmpdir, random_metadata):
    random_metadata['hash'] = '1234'
    f = random_file(tmpdir, metadata=random_metadata)
    assert f.metadata['hash'] == random_metadata['hash']


def test_default_where(monkeypatch, tmpdir, random_metadata):
    monkeypatch.setenv('DATALAKE_DEFAULT_WHERE', 'here')
    del(random_metadata['where'])
    f = random_file(tmpdir, metadata=random_metadata)
    assert f.metadata['where'] == 'here'


def test_valid_bundle(tmpdir, random_metadata):
    p = os.path.join(str(tmpdir), 'foo.tar')
    f1 = random_file(tmpdir, metadata=random_metadata)
    f1.to_bundle(p)
    f2 = File.from_bundle(p)
    assert f1.metadata == f2.metadata
    content1 = f1.read()
    content2 = f2.read()
    assert content1
    assert content1 == content2


def test_bundle_not_tar(tmpfile):
    f = tmpfile('foobar')
    with pytest.raises(InvalidDatalakeBundle):
        File.from_bundle(f)


def add_string_to_tar(tfile, arcname, data):
    if data is None:
        return
    s = StringIO(data)
    info = tarfile.TarInfo(name=arcname)
    s.seek(0, os.SEEK_END)
    info.size = s.tell()
    s.seek(0, 0)
    tfile.addfile(tarinfo=info, fileobj=s)


@pytest.fixture
def bundle_maker(tmpdir):

    def maker(content=None, metadata=None, version=None):
        f = random_word(10) + '.tar'
        f = os.path.join(str(tmpdir), f)
        t = tarfile.open(f, 'w')
        add_string_to_tar(t, 'content', content)
        add_string_to_tar(t, 'version', version)
        add_string_to_tar(t, 'datalake-metadata.json', metadata)
        t.close()
        return f

    return maker


def test_bundle_without_version(bundle_maker, random_metadata):
    m = json.dumps(random_metadata)
    b = bundle_maker(content='1234', metadata=m)
    with pytest.raises(InvalidDatalakeBundle):
        File.from_bundle(b)


def test_bundle_without_metadata(bundle_maker):
    b = bundle_maker(content='1234', version='0')
    with pytest.raises(InvalidDatalakeBundle):
        File.from_bundle(b)


def test_bundle_without_content(bundle_maker, random_metadata):
    m = json.dumps(random_metadata)
    b = bundle_maker(metadata=m, version='0')
    with pytest.raises(InvalidDatalakeBundle):
        File.from_bundle(b)


def test_bundle_with_non_json_metadata(bundle_maker):
    b = bundle_maker(content='1234', metadata='not:a%json#', version='0')
    with pytest.raises(InvalidDatalakeBundle):
        File.from_bundle(b)


def test_bundle_with_invalid_metadata(bundle_maker, random_metadata):
    del(random_metadata['what'])
    m = json.dumps(random_metadata)
    b = bundle_maker(content='1234', metadata=m, version='0')
    with pytest.raises(InvalidDatalakeMetadata):
        File.from_bundle(b)

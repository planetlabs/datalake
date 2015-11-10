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

from datalake import File


def random_file(tmpdir, metadata=None):
    name = random_word(10)
    content = random_word(256)
    f = tmpdir.join(name)
    f.write(content)
    if metadata is None:
        metadata = random_metadata()
    return File(f.strpath, **metadata)


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
        File('surelythisfiledoesnotexist.txt')


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

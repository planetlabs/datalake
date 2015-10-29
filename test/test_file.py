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

import pytest
from . import random_word

from datalake import File


def random_file(tmpdir):
    name = random_word(10)
    content = random_word(256)
    f = tmpdir.join(name)
    f.write(content)
    return File(f.strpath)

@pytest.fixture
def random_files(tmpdir):
    def get_randfiles(n):
        return [random_file(tmpdir) for _ in range(n)]
    return get_randfiles

def test_file_hash_different(random_files):
    files = random_files(2)
    assert files[0].hash != files[1].hash

def test_non_existent_file():
    with pytest.raises(IOError):
        File('surelythisfiledoesnotexist.txt')

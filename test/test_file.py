import pytest
import random
import string
from datetime import datetime, timedelta

from datalake import File

def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def random_interval():
    now = datetime.now()
    start = now - timedelta(days=random.randint(0, 365*3))
    end = start - timedelta(days=random.randint(1, 10))
    return start.isoformat(), end.isoformat()

def random_metadata():
    start, end = random_interval()
    return {
        'version': 0,
        'start': start,
        'end': end,
        'where': random_word(10),
        'what': random_word(10),
    }

def random_file(tmpdir):
    name = random_word(10)
    content = random_word(256)
    f = tmpdir.join(name)
    f.write(content)
    return File(f.strpath, random_metadata())

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
        File('surelythisfiledoesnotexist.txt', random_metadata())

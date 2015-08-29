import pytest
import random
import string
from datetime import datetime, timedelta
from moto import mock_s3
import boto


@pytest.fixture
def basic_metadata():

    return {
        'version': 0,
        'start': 1426809600000,
        'end': 1426895999999,
        'where': 'nebraska',
        'what': 'apache',
        'hash': '12345'
    }

def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in xrange(length))

def random_interval():
    now = datetime.now()
    start = now - timedelta(days=random.randint(0, 365*3))
    end = start - timedelta(days=random.randint(1, 10))
    return start.isoformat(), end.isoformat()

@pytest.fixture
def random_metadata():
    start, end = random_interval()
    return {
        'version': 0,
        'start': start,
        'end': end,
        'where': random_word(10),
        'what': random_word(10),
    }

@pytest.fixture
def tmpfile(tmpdir):
    name = random_word(10)
    def get_tmpfile(content):
        f = tmpdir.join(name)
        f.write(content)
        return str(f)

    return get_tmpfile

@pytest.fixture
def s3_conn(request):
    mock = mock_s3()
    mock.start()
    conn = boto.connect_s3()

    def tear_down():
        mock.stop()
    request.addfinalizer(tear_down)

    return conn

BUCKET_NAME = 'datalake-test'

@pytest.fixture
def s3_bucket(s3_conn):
    return s3_conn.create_bucket(BUCKET_NAME)

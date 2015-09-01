import pytest
import random
import string
from datetime import datetime, timedelta


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

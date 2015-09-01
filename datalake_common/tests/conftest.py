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

def random_hex(length):
    return ('%0' + str(length) + 'x') % random.randrange(16**length)

def random_interval():
    now = datetime.now()
    start = now - timedelta(days=random.randint(0, 365*3))
    end = start - timedelta(days=random.randint(1, 10))
    return start.isoformat(), end.isoformat()

def random_work_id():
    if random.randint(0, 1):
        return None
    return '{}-{}'.format(random_word(5), random.randint(0,2**15))

@pytest.fixture
def random_metadata():
    start, end = random_interval()
    return {
        'version': 0,
        'start': start,
        'end': end,
        'work_id': random_work_id(),
        'where': random_word(10),
        'what': random_word(10),
        'id': random_hex(40),
        'hash': random_hex(40),
    }

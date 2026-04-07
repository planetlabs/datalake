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
import random
import string
import os
import six


try:
    from moto import mock_s3
    import boto3
    from six.moves.urllib.parse import urlparse
    import json
except ImportError:
    # if developers use s3-test features without having installed s3 stuff,
    # things will fail. So it goes.
    pass


@pytest.fixture
def basic_metadata():

    return {
        'version': 0,
        'start': 1426809600000,
        'end': 1426895999999,
        'path': '/var/log/apache/access.log',
        'where': 'nebraska',
        'what': 'apache',
        'hash': '12345',
        'work_id': None,
    }


def random_word(length):
    if six.PY2:
        lowercase = string.lowercase
    else:
        lowercase = string.ascii_lowercase
    return ''.join(random.choice(lowercase) for i in range(length))


def random_hex(length):
    return ('%0' + str(length) + 'x') % random.randrange(16**length)


def random_interval():
    year_2010 = 1262304000000
    five_years = 5 * 365 * 24 * 60 * 60 * 1000
    three_days = 3 * 24 * 60 * 60 * 1000
    start = year_2010 + random.randint(0, five_years)
    end = start + random.randint(0, three_days)
    return start, end


def random_work_id():
    if random.randint(0, 1):
        return None
    return '{}-{}'.format(random_word(5), random.randint(0, 2**15))


def random_abs_dir():
    num_dirs = random.randrange(1, 4)
    lengths = [random.randint(2, 10) for i in range(num_dirs)]
    dirs = [random_word(i) for i in lengths]
    return '/' + '/'.join(dirs)


@pytest.fixture
def random_metadata():
    return generate_random_metadata()


def generate_random_metadata():
    start, end = random_interval()
    what = random_word(10)
    return {
        'version': 0,
        'start': start,
        'end': end,
        'path': os.path.join(random_abs_dir(), what),
        'work_id': random_work_id(),
        'where': random_word(10),
        'what': what,
        'id': random_hex(40),
        'hash': random_hex(40),
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
def tmpfile_maker(tmpdir):

    def get_tmpfile(content):
        name = random_word(10)
        f = tmpdir.join(name)
        f.write(content)
        return str(f)

    return get_tmpfile


@pytest.fixture
def aws_connector(request):

    def create_connection(mocker, connector):
        mock = mocker()
        mock.start()

        def tear_down():
            mock.stop()
        request.addfinalizer(tear_down)

        return connector()

    return create_connection


@pytest.fixture
def s3_connection(aws_connector):
    with mock_s3():
        yield boto3.resource('s3')


@pytest.fixture
def s3_bucket_maker(s3_connection):

    def maker(bucket_name):
        b = s3_connection.Bucket(bucket_name)
        b.create()
        return b

    return maker


@pytest.fixture
def s3_file_maker(s3_bucket_maker):

    def maker(bucket, key, content, metadata):
        b = s3_bucket_maker(bucket)
        b.Object(key).put(
            Body=content,
            Metadata={'datalake': json.dumps(metadata)} if metadata else {}
        )

    return maker


@pytest.fixture
def s3_file_from_metadata(s3_file_maker):

    def maker(url, metadata):
        url = urlparse(url)
        assert url.scheme == 's3'
        # NB: clean up leading slash.
        s3_file_maker(url.netloc, url.path.lstrip('/'), '', metadata)

    return maker

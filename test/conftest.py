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
from moto import mock_s3
import boto
from urlparse import urlparse
from datalake_common.tests import random_metadata, tmpfile  # noqa

from datalake import Archive


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


@pytest.fixture
def archive(s3_bucket):
    bucket_url = 's3://' + s3_bucket.name + '/'
    return Archive(bucket_url)


@pytest.fixture
def s3_key(s3_conn):

    def get_s3_key(url):
        url = urlparse(url)
        assert url.scheme == 's3'
        bucket = s3_conn.get_bucket(url.netloc)
        return bucket.get_key(url.path)

    return get_s3_key

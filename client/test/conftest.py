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
import boto3
import botocore.exceptions
from six.moves.urllib.parse import urlparse
from datalake.tests import random_metadata, tmpfile  # noqa
import os
from click.testing import CliRunner
import stat
import responses
import logging

from datalake.scripts.cli import cli
from datalake import Archive


logging.basicConfig(level=logging.INFO)


# If we run with proper AWS credentials they will be used
# This will cause moto to fail
# But more critically, may impact production systems
# So we test for real credentials and fail hard if they exist
sts = boto3.client('sts')
try:
    sts.get_caller_identity()
    pytest.exit("Real AWS credentials detected, aborting", 3)
except botocore.exceptions.NoCredentialsError:
    pass  # no credentials are good


@pytest.fixture(scope="function")
def s3_conn():
    with mock_s3():
        resource = boto3.resource('s3')
        yield resource

BUCKET_NAME = 'datalake-test'

@pytest.fixture
def s3_bucket(s3_conn):
    b = s3_conn.Bucket(BUCKET_NAME)
    b.create()
    return b

@pytest.fixture
def archive_maker(s3_bucket):

    def maker(**kwargs):
        kwargs.update(
            storage_url='s3://' + s3_bucket.name + '/',
            http_url='http://datalake.example.com'
        )
        return Archive(**kwargs)

    return maker


@pytest.fixture
def archive(archive_maker):
    return archive_maker()


@pytest.fixture
def s3_obj(s3_conn, s3_bucket):

    def get_s3_obj(url=None):
        if url is None:
            # if no url is specified, assume there is just one key in the
            # bucket. This is the common case for tests that only push one
            # item.
            objs = s3_bucket.objects.all()
            count = sum(1 for _ in objs)  # without retrieving all
            assert count == 1
            iter_objs = iter(objs)
            return next(iter_objs).Object()
        else:
            url = urlparse(url)
            assert url.scheme == 's3'
            return s3_conn.Object(url.netloc, url.path[1:])

    return get_s3_obj


@pytest.fixture
def cli_tester(s3_bucket):

    def tester(command, expected_exit=0):
        os.environ['DATALAKE_STORAGE_URL'] = 's3://' + s3_bucket.name
        os.environ['DATALAKE_HTTP_URL'] = 'http://datalake.example.com'
        parts = command.split(' ')
        runner = CliRunner()
        result = runner.invoke(cli, parts, catch_exceptions=False)
        assert result.exit_code == expected_exit, result.output
        return result.output

    return tester


@pytest.fixture  # noqa
def datalake_url_maker(archive, tmpfile, random_metadata):

    def maker(metadata=random_metadata, content=b''):
        f = tmpfile(content)
        return archive.prepare_metadata_and_push(f, **metadata)

    return maker


crtime = os.environ.get('CRTIME', '/usr/local/bin/crtime')
crtime_available = os.path.isfile(crtime) and os.access(crtime, os.X_OK)
crtime_setuid = False
if crtime_available:
    s = os.stat(crtime)
    crtime_setuid = s.st_mode & stat.S_ISUID and s.st_uid == 0


def prepare_response(response, status=200, url=None, **query_params):
    url = url or 'http://datalake.example.com/v0/archive/files/'
    if len(query_params):
        q = ['{}={}'.format(k, query_params[k]) for k in query_params.keys()]
        url = url + '?' + '&'.join(q)
    responses.add(responses.GET, url, json=response, status=status,
                  match_querystring=True)

import pytest
from tempfile import NamedTemporaryFile
import boto
from moto import mock_s3
from urlparse import urlparse
import simplejson as json

from datalake import File, Archive

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

def test_push_file(archive, random_metadata, tmpfile, s3_key):
    expected_content = 'mwahaha'
    f = tmpfile(expected_content)
    url = archive.push(f, **random_metadata)
    from_s3 = s3_key(url)
    assert from_s3.get_contents_as_string() == expected_content
    metadata = from_s3.get_metadata('datalake')
    assert metadata is not None
    metadata = json.loads(metadata)
    common_keys = set(metadata.keys()).intersection(random_metadata.keys())
    assert common_keys == set(random_metadata.keys())

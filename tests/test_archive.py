import pytest
from tempfile import NamedTemporaryFile
from urlparse import urlparse
import simplejson as json

from datalake import File, Archive


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

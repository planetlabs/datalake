from unittest import TestCase
from tempfile import NamedTemporaryFile
import boto
from moto import mock_s3
from urlparse import urlparse
import simplejson as json

from datalake import File, Archive


class TestArchiveS3Tests(TestCase):

    def setUp(self):
        self.mock = mock_s3()
        self.mock.start()

        self.metadata = {
            'version': '0',
            'start': '2015-03-20T00:00:00Z',
            'end': '2015-03-20T23:59:59.999Z',
            'where': 'nebraska',
            'what': 'apache',
        }
        self.bucket_name = 'datalake-test'
        self.bucket_url = 's3://' + self.bucket_name + '/'
        self.datalake = Archive(self.bucket_url)
        self.conn = boto.connect_s3()
        self.conn.create_bucket(self.bucket_name)

    def tearDown(self):
        self.mock.stop()

    def create_archived_file(self, content, metadata):
        with NamedTemporaryFile() as tf:
            tf.write(content)
            tf.flush()
            f = File(tf.name, metadata)
            url = self.datalake.push(f)
            return url, f

    def get_s3_key(self, url):
        url = urlparse(url)
        self.assertEqual(url.scheme, 's3')
        bucket = self.conn.get_bucket(url.netloc)
        return bucket.get_key(url.path)

    def test_push_file(self):
        expected_content = 'mwahaha'
        url, f = self.create_archived_file(expected_content, self.metadata)
        from_s3 = self.get_s3_key(url)
        self.assertEqual(from_s3.get_contents_as_string(), expected_content)
        metadata = from_s3.get_metadata('datalake')
        self.assertIsNotNone(metadata)
        self.assertDictEqual(json.loads(metadata), f.metadata)

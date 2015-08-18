from conf import get_config
import urlparse
from memoized_property import memoized_property
import simplejson as json

from boto.s3.connection import S3Connection
from boto.s3.key import Key


class UnsupportedStorageError(Exception):
    pass


class Archive(object):

    def __init__(self, storage_url=None, **kwargs):
        self.storage_url = storage_url or get_config().storage_url
        self._validate_storage_url()

    def _validate_storage_url(self):
        if not self._parsed_storage_url.scheme == 's3':
            msg = 'Unsupported storage scheme ' + \
                self._parsed_url.scheme
            raise UnsupportedStorageError(msg)

    @property
    def _parsed_storage_url(self):
        return urlparse.urlparse(self.storage_url)

    def push(self, f):
        '''push a file f to the archive

        returns the url to which the file was pushed.
        '''
        key = self._s3_key_from_metadata(f)
        key.set_metadata('datalake', json.dumps(f.metadata))
        key.set_contents_from_string(f.read())
        return self._get_s3_url(f)

    _URL_FORMAT = 's3://{bucket}/{key}'

    def _get_s3_url(self, f):
        key = self._s3_key_from_metadata(f)
        return self._URL_FORMAT.format(bucket=self._s3_bucket_name,
                                       key=key.name)

    @property
    def _s3_bucket_name(self):
        return self._parsed_storage_url.netloc

    @memoized_property
    def _s3_bucket(self):
        return self._s3_conn.get_bucket(self._s3_bucket_name)

    _KEY_FORMAT = '{prefix}-{where}/{what}/{start}/{id}-{name}'

    def _s3_key_from_metadata(self, f):
        # For performance reasons, s3 keys should start with a short random
        # sequence:
        # https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/
        # http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html
        name = f._basename
        key_name = self._KEY_FORMAT.format(name=name,
                                           prefix=f.hash[0],
                                           **f.metadata)
        return Key(self._s3_bucket, name=key_name)

    @property
    def _s3_conn(self):
        if not hasattr(self, '_conn'):
            self._conn = S3Connection()
        return self._conn

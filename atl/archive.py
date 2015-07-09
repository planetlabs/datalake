from conf import get_config
import urlparse
from memoized_property import memoized_property
import os
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

    def push(self, log):
        '''push a log to the archive

        returns the url to which the log was pushed. The log's metadata is also
        updated with the url.
        '''
        log.metadata['url'] = self._get_s3_url(log)
        key = self._s3_key_from_metadata(log)
        key.set_metadata('atl', json.dumps(log.metadata))
        key.set_contents_from_filename(log.path)
        return log.metadata['url']

    _URL_FORMAT = 's3://{bucket}/{key}'

    def _get_s3_url(self, log):
        key = self._s3_key_from_metadata(log)
        return self._URL_FORMAT.format(bucket=self._s3_bucket_name,
                                       key=key.name)

    @property
    def _s3_bucket_name(self):
        return self._parsed_storage_url.netloc

    @memoized_property
    def _s3_bucket(self):
        return self._s3_conn.get_bucket(self._s3_bucket_name)

    _KEY_FORMAT = '{where}/{what}/{name}'

    def _s3_key_from_metadata(self, log):
        name = os.path.basename(log.path)
        key_name = self._KEY_FORMAT.format(name=name, **log.metadata)
        return Key(self._s3_bucket, name=key_name)

    @property
    def _s3_conn(self):
        if not hasattr(self, '_conn'):
            self._conn = S3Connection()
        return self._conn

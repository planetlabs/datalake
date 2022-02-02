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

import os
import re
from os import environ
from six.moves.urllib.parse import urlparse
from memoized_property import memoized_property
import simplejson as json
from datalake import (
    File,
    StreamingFile,
    StreamingHTTPFile,
)
from .common.errors import InsufficientConfiguration
from .common import Metadata
import requests
from io import BytesIO
import errno

import boto3
import botocore.exceptions

import math
from logging import getLogger
log = getLogger('datalake-archive')

# The name in s3 of the datalake metadata document
METADATA_NAME = 'datalake'

MB_B = 1024 ** 2


# S3 limit for PUT request is 5GB
# S3 limit for minimum multipart upload size is 5MB
def CHUNK_SIZE_BYTES():
    return int(float(os.getenv('DATALAKE_CHUNK_SIZE_MB', 100)) * MB_B)


_connect_timeout = None
_read_timeout = None


def TIMEOUT():
    """
    A tuple of the connect and read timeout, with defaults using the
    recommendations from the requests docs:
     https://requests.readthedocs.io/en/master/user/advanced/#timeouts

    :return: tuple of floats
    """
    return CONNECT_TIMEOUT(), READ_TIMEOUT(),


def CONNECT_TIMEOUT():
    global _connect_timeout
    if _connect_timeout is None:
        _connect_timeout = float(os.getenv('DATALAKE_CONNECT_TIMEOUT_S', 3.05))
    return _connect_timeout


def READ_TIMEOUT():
    global _read_timeout
    if _read_timeout is None:
        _read_timeout = float(os.getenv('DATALAKE_READ_TIMEOUT_S', 31))
    return _read_timeout


class UnsupportedStorageError(Exception):
    pass


class DatalakeHttpError(Exception):
    pass


class InvalidDatalakePath(Exception):
    pass


class Archive(object):

    def __init__(self, storage_url=None, http_url=None, session=None):
        self.storage_url = storage_url or environ.get('DATALAKE_STORAGE_URL')
        self._validate_storage_url()
        self._http_url = http_url
        self.__session = session

    def _validate_storage_url(self):
        if not self.storage_url:
            raise UnsupportedStorageError('Please specify a storage URL')

        if not self._parsed_storage_url.scheme == 's3':
            msg = 'Unsupported storage scheme ' + \
                self._parsed_storage_url.scheme
            raise UnsupportedStorageError(msg)

        self.storage_url = self.storage_url.rstrip('/')

    @property
    def _parsed_storage_url(self):
        return urlparse(self.storage_url)

    def list(self, what, start=None, end=None, where=None, work_id=None):
        '''list metadata records for specified files

        Args:
          what: what kind of file to list (e.g., syslog, nginx)

          start: List only files after this time. This argument is
          polymorphic. datetimes are accepted. Strings will be converted to
          datetimes, so inputs like `2015-12-21` and `2015-12-21T09:11:14.08Z`
          are acceptable. Floats will be interpreted as UTC seconds since the
          epoch. Integers will be interpreted as milliseconds since the epoch.

          end: List only files before this time. Same semantics as start.

          where: List only files from this host.

          work_id: Show only files with this work id.

        returns a generator that lists records of the form:
            {
                'url': <url>,
                'metadata': <metadata>,
            }
        '''
        url = self.http_url + '/v0/archive/files/'
        params = dict(
            what=what,
            start=None if start is None else Metadata.normalize_date(start),
            end=None if end is None else Metadata.normalize_date(end),
            where=where,
            work_id=work_id,
        )
        response = self._requests_get(url, params=params)

        while True:
            self._check_http_response(response)
            response = response.json()
            for record in response['records']:
                yield record
            if response['next']:
                response = self._requests_get(response['next'])
            else:
                break

    def latest(self, what, where, lookback=None):
        url = self.http_url + '/v0/archive/latest/{}/{}'.format(what, where)
        params = dict(
            lookback=lookback,
        )
        response = self._requests_get(url, params=params)
        self._check_http_response(response)
        return response.json()

    @property
    def http_url(self):
        self._http_url = self._http_url or environ.get('DATALAKE_HTTP_URL')
        if self._http_url is None:
            raise InsufficientConfiguration('Please specify DATALAKE_HTTP_URL')
        return self._http_url.rstrip('/')

    def _check_http_response(self, response):
        if response.status_code in (400, 404):
            err = response.json()
            msg = '{} ({})'.format(err['message'], err['code'])
            raise DatalakeHttpError(msg)

        elif response.status_code != 200:
            msg = 'Datalake HTTP API failed: {} ({})'
            msg = msg.format(response.content, response.status_code)
            raise DatalakeHttpError(msg)

    def prepare_metadata_and_push(self, filename, **metadata_fields):
        '''push a file to the archive with the specified metadata

        Args:
            filename: path of the file to push

            metadata_fields: metadata fields for file. Missing fields will be
            added if they can be determined. Othwerise, InvalidDatalakeMetadata
            will be raised.

        returns the url to which the file was pushed.
        '''
        f = File.from_filename(filename, **metadata_fields)
        return self.push(f)


    def push(self, f):
        '''push a file f to the archive

        Args:
            f is a datalake.File

        returns the url to which the file was pushed.
        '''
        s3obj = self._s3_object_from_metadata(f)

        config = boto3.s3.transfer.TransferConfig(
                # All sizes are bytes
                multipart_threshold = max(8 * MB_B, CHUNK_SIZE_BYTES()),
                use_threads = True,
                max_concurrency = 10,
                multipart_chunksize = CHUNK_SIZE_BYTES(),
        )

        extra = {
                'Metadata': {
                    METADATA_NAME: json.dumps(f.metadata)
                }
        }

        s3obj.upload_fileobj(f, ExtraArgs=extra, Config=config)
        # TODO: Need to check partial transfer behaviour, ensure we clean up

        return self._s3_object_to_s3_url(s3obj)


    @staticmethod
    def _s3_object_to_s3_url(s3obj):
        return "s3://{}/{}".format(s3obj.bucket_name, s3obj.key)

    def url_from_file(self, f):
        s3obj = self._s3_object_from_metadata(f)
        return self._s3_object_to_s3_url(s3obj)

    def fetch(self, url, stream=False):
        '''fetch the specified url and return it as a datalake.File

        Args:

        url: the url to fetch. Both s3 and http(s) are supported.
             the http URL should point to a datalake API server address
        stream: if true, return a StreamingFile
        '''
        if url.lower().startswith('s3://'):
            try:
                return self._file_from_s3_url(url, stream=stream)
            except botocore.exceptions.ClientError as e:
                # Captures a variety of boto errors, so we pick and choose
                if e.response['Error']['Code'] == '404':
                    raise InvalidDatalakePath(url)
                else:
                    raise
        elif url.startswith('http') and url.endswith('/data'):
            return self._file_from_http_url(url, stream=stream)
        else:
            msg = '{} does not appear to be a fetchable url'
            msg = msg.format(url)
            raise InvalidDatalakePath(msg)

    def _file_from_s3_url(self, url, stream=False):
        s3_obj = self._s3_object_from_s3_url(url)
        return self._file_from_s3_object(s3_obj, stream)

    def _file_from_s3_object(self, s3_obj, stream=False):
        metadata = self._get_metadata_from_s3_object(s3_obj)
        if stream:
            return StreamingFile(s3_obj.get()['Body'], **metadata)
        fd = BytesIO()
        s3_obj.download_fileobj(fd)
        fd.seek(0)
        return File(fd, **metadata)

    def _file_from_http_url(self, url, stream=False):
        # Hitting the datalake api server
        # Data URL like https://datalake.earth.planet.com/v0/archive/files/{fileid}/data
        # Metadata is retrieved from the same, but /data is /metadata

        data_response = self._stream_http_url(url)
        metadata = self._get_metadata_from_http_url(url)

        if stream:
            return StreamingHTTPFile(data_response, **metadata)

        fd = BytesIO()
        for block in data_response.iter_content(1024):
            fd.write(block)

        fd.seek(0)
        return File(fd, **metadata)

    def _stream_http_url(self, url):
        response = self._requests_get(url, stream=True)
        self._check_http_response(response)
        return response

    def _get_metadata_from_http_url(self, url):
        self._validate_http_url(url)
        p = re.compile('/data$')
        url = p.sub('/metadata', url)
        response = self._requests_get(url, stream=True)
        self._check_http_response(response)
        return response.json()

    def _http_url_to_disk(self, url, filename_template=None):
        metadata = self._get_metadata_from_http_url(url)

        fname = self._get_filename_from_template(filename_template, metadata)
        self._mkdirs(os.path.dirname(fname))

        with open(fname, 'wb') as fh:
            for buf in self.fetch(url, stream=True).iter_content():
                fh.write(buf)

        return fname

    def _s3_url_to_disk(self, url, filename_template=None):
        s3obj = self._s3_object_from_s3_url(url)
        metadata = self._get_metadata_from_s3_object(s3obj)

        fname = self._get_filename_from_template(filename_template, metadata)
        self._mkdirs(os.path.dirname(fname))

        s3obj.download_file(fname)

        return fname

    def fetch_to_filename(self, url, filename_template=None):
        '''fetch the specified url and write it to a file

        Args:

        url: the url to fetch

        filename_template: a template describing where to store files. For
        example, to store a file based on `what` it is, you could pass
        something like {what}.log. Or if you gathering many `what`'s that
        originate from many `where`'s you might want to use something like
        {where}/{what}-{start}.log. If filename_template is None (the default),
        files are stored in the current directory and the filenames are the ids
        from the metadata.

        Returns the filename written.
        '''

        if url.startswith('s3://'):
            return self._s3_url_to_disk(url, filename_template)
        else:
            return self._http_url_to_disk(url, filename_template)


    def _mkdirs(self, path):
        if path == '':
            return
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    @staticmethod
    def split_s3_url(url):
        # Returns tuple ('bucketname', 'keyname')
        if url.startswith('s3://'):
            url = url[5:]
        else:
            raise InvalidDatalakePath(url)
        if '/' not in url:
            raise InvalidDatalakePath(url)

        return url.split('/', maxsplit=1)


    def _s3_object_from_s3_url(self, url):
        # URL must be s3://...
        bucket_name, key_name = self.split_s3_url(url)
        return self._s3.Object(bucket_name, key_name)
        # NOTE: object may not exist, exception will be raised on access

    _KEY_FORMAT = '{id}/data'
    def _s3_object_from_metadata(self, f):
        # For performance reasons, s3 keys should start with a short random
        # sequence:
        # https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/
        # http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html
        key_name = self._KEY_FORMAT.format(**f.metadata)
        return self._s3_bucket.Object(key_name)

    def _get_metadata_from_s3_object(self, s3_obj):
        return Metadata.from_json(s3_obj.metadata.get(METADATA_NAME))

    def _get_filename_from_template(self, template, metadata):
        if template is None:
            template = '{id}'
        try:
            return template.format(**metadata)
        except KeyError as e:
            m = '"{}" does not appear in the datalake metadata'
            m = m.format(str(e))
            raise InvalidDatalakePath(m)
        except ValueError as e:
            raise InvalidDatalakePath(str(e))

    def _get_key_name_from_url(self, url):
        parts = urlparse(url)
        if not parts.path:
            msg = '{} is not a valid datalake url'.format(url)
            raise InvalidDatalakePath(msg)

        return parts.path

    def _validate_http_url(self, url):
        if not url.startswith(self.http_url):
            msg = 'url {} does not start with the configured storage url {}.'
            msg = msg.format(url, self.http_url)
            raise InvalidDatalakePath(msg)


    @property
    def _s3_bucket_name(self):
        return self._parsed_storage_url.netloc

    @memoized_property
    def _s3_bucket(self):
        # TODO: Ensure we can push without list permissions
        return self._s3.Bucket(self._s3_bucket_name)


    @memoized_property
    def _s3(self):
        # boto3 uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        # boto3 will use AWS_DEFAULT_REGION if AWS_REGION is not set
        return boto3.resource('s3', region_name=environ.get('AWS_REGION'))

    def _requests_get(self, url, **kwargs):
        return self._session.get(url, timeout=TIMEOUT(), **kwargs)

    @property
    def _session(self):
        if self.__session:
            return self.__session

        session_class = os.environ.get('DATALAKE_SESSION_CLASS')
        if session_class is not None:
            import importlib
            parts = session_class.split('.')
            module_name = '.'.join(parts[:-1])
            class_name = parts[-1]
            module = importlib.import_module(module_name)
            session_class = getattr(module, class_name)
            self.__session = session_class()
        else:
            self.__session = requests.Session()
        return self.__session

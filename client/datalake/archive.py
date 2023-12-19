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
import json
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
from copy import deepcopy
from datetime import datetime

import boto3
import math
from logging import getLogger
log = getLogger('datalake-archive')

# The name in s3 of the datalake metadata document
METADATA_NAME = 'datalake'

MB_B = 1024 ** 2


# S3 limit for PUT request is 5GB
# S3 limit for minimum multipart upload size is 5MB
def CHUNK_SIZE():
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
        self._upload_file(f)
        return self.url_from_file(f)

    def _upload_file(self, f):

        # Implementation inspired by https://stackoverflow.com/a/60892027
        obj = self._s3_object_from_metadata(f)

        # NB: we have an opportunitiy to turn on threading here, which may
        # improve performance. However, in some cases (i.e., queue-based
        # uploader) we already use threads. So let's add it later as a
        # configuration if/when we want to experiment.
        config = boto3.s3.transfer.TransferConfig(
            # All sizes are bytes
            multipart_threshold=CHUNK_SIZE(),
            use_threads=False,
            multipart_chunksize=CHUNK_SIZE(),
        )

        extra = {
            'Metadata': {
                METADATA_NAME: json.dumps(f.metadata)
            }
        }

        spos = f.tell()
        f.seek(0, os.SEEK_END)
        f_size = f.tell()
        # seek back to the correct position.
        f.seek(spos)

        num_chunks = int(math.ceil(f_size / float(CHUNK_SIZE())))
        log.info("Uploading {} ({} B / {} chunks)".format(
            obj.key, f_size, num_chunks))

        chunk = 0

        def _progress(number_of_bytes):
            nonlocal chunk
            log.info("Uploaded chunk {}/{} ({}B)".format(
                chunk, num_chunks, CHUNK_SIZE()))
            chunk += 1

        # NB: deep under the hood, upload_fileobj creates a
        # CreateMultipartUploadTask. And that object cleans up after itself:
        # https://github.com/boto/s3transfer/blob/develop/s3transfer/tasks.py#L353-L360  # noqa
        obj.upload_fileobj(f, ExtraArgs=extra, Config=config,
                           Callback=_progress)
        obj.wait_until_exists()

    def url_from_file(self, f):
        return self._get_s3_url(f)

    _URL_FORMAT = 's3://{bucket}/{key}'

    def fetch(self, url, stream=False):
        '''fetch the specified url and return it as a datalake.File

        Args:

        url: the url to fetch. Both s3 and http(s) are supported.
        stream: if true, return a StreamingFile
        '''
        if url.startswith('s3://'):
            return self._fetch_s3_url(url, stream=stream)
        elif self._is_valid_http_url(url):
            return self._fetch_http_url(url, stream=stream)
        else:
            msg = '{} does not appear to be a fetchable url'
            msg = msg.format(url)
            raise InvalidDatalakePath(msg)

    def _is_valid_http_url(self, url):
        return url.startswith('http') and url.endswith('/data')

    def _fetch_s3_url(self, url, stream=False):
        obj, m = self._get_object_from_url(url)
        if stream:
            return StreamingFile(obj._datalake_details['Body'], **m)
        fd = BytesIO()
        self._s3_bucket.download_fileobj(obj.key, fd)
        fd.seek(0)
        return File(fd, **m)

    def _fetch_http_url(self, url, stream=False):
        m = self._get_metadata_from_http_url(url)
        k = self._stream_http_url(url)
        if stream:
            return StreamingHTTPFile(k, **m)
        fd = BytesIO()
        for block in k.iter_content(1024):
            fd.write(block)
        fd.seek(0)
        return File(fd, **m)

    def _stream_http_url(self, url):
        response = self._requests_get(url, stream=True)
        self._check_http_response(response)
        return response

    def _get_metadata_from_http_url(self, url):
        self._validate_fetch_url(url)
        p = re.compile('/data$')
        url = p.sub('/metadata', url)
        response = self._requests_get(url, stream=True)
        self._check_http_response(response)
        return response.json()

    def fetch_to_filename(self, url, filename_template=None):
        '''fetch the specified url and write it to a file

        Args:

        url: the url to fetch

        filename_template: a template describing where to store files. For
        example, to store a file based on `what` it is, you could pass
        something like {what}.log. Or if you gathering many `what`'s that
        originate from many `where`'s you might want to use something like
        {where}/{what}-{start}.log. Note that the template variables
        {start_iso} and {end_iso} are also supported and expand to the ISO
        timestamps with millisecond precision (e.g., 2023-12-19T00:10:22.123).

        If filename_template is None (the default), files are stored in the
        current directory and the filenames are the ids from the metadata.

        Returns the filename written.

        '''
        k = None
        if url.startswith('s3://'):
            obj, m = self._get_object_from_url(url)
        else:
            m = self._get_metadata_from_http_url(url)
        fname = self._get_filename_from_template(filename_template, m)
        dname = os.path.dirname(fname)
        self._mkdirs(dname)
        if k:
            k.get_contents_to_filename(fname)
        else:
            with open(fname, 'wb') as fh:
                for buf in self.fetch(url, stream=True).iter_content():
                    fh.write(buf)
        return fname

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

    def _get_object_from_url(self, url):
        self._validate_fetch_url(url)
        key_name = self._get_key_name_from_url(url)
        obj = self._s3.Object(self._s3_bucket_name, key_name)
        try:
            # cache the results of the get on the obj to avoid superfluous
            # network calls.
            obj._datalake_details = obj.get()
            m = obj._datalake_details['Metadata'].get(METADATA_NAME)
        except self._s3.meta.client.exceptions.NoSuchKey:
            msg = 'Failed to find {} in the datalake.'.format(url)
            raise InvalidDatalakePath(msg)
        return obj, Metadata.from_json(m)

    def _get_filename_from_template(self, template, metadata):
        template_vars = deepcopy(metadata)
        template_vars.update(
            start_iso=self._ms_to_iso(metadata.get('start')),
            end_iso=self._ms_to_iso(metadata.get('end')),
        )
        if template is None:
            template = '{id}'
        try:
            return template.format(**template_vars)
        except KeyError as e:
            m = '"{}" does not appear to be a supported template variable.'
            m = m.format(str(e))
            raise InvalidDatalakePath(m)
        except ValueError as e:
            raise InvalidDatalakePath(str(e))

    _ISO_FORMAT_MS = '%Y-%m-%dT%H:%M:%S.%f'

    def _ms_to_iso(self, ts):
        if ts is None:
            return None
        d = datetime.utcfromtimestamp(ts/1000.0)
        # drop to ms precision
        return d.strftime(self._ISO_FORMAT_MS)[:-3]

    def _get_key_name_from_url(self, url):
        parts = urlparse(url)
        if not parts.path:
            msg = '{} is not a valid datalake url'.format(url)
            raise InvalidDatalakePath(msg)

        # NB: under boto 2 we didn't used to have to have the lstrip. It seems
        # that boto2 explicitly stripped these leading slashes for us:
        # https://groups.google.com/g/boto-users/c/mv--NMPUXoU ...but boto3
        # does not. So we must take care to strip it whenever we parse a URL to
        # get a key.
        return parts.path.lstrip('/')

    def _validate_fetch_url(self, url):
        valid_base_urls = (self.storage_url, self.http_url)
        if not [u for u in valid_base_urls if url.startswith(u)]:
            msg = 'url {} does not start with the configured storage urls {}.'
            msg = msg.format(url, valid_base_urls)
            raise InvalidDatalakePath(msg)

    def _get_s3_url(self, f):
        obj = self._s3_object_from_metadata(f)
        return self._URL_FORMAT.format(bucket=self._s3_bucket_name,
                                       key=obj.key)

    @property
    def _s3_bucket_name(self):
        return self._parsed_storage_url.netloc

    @memoized_property
    def _s3_bucket(self):
        return self._s3.Bucket(self._s3_bucket_name)

    _KEY_FORMAT = '{id}/data'

    def _s3_object_from_metadata(self, f):
        key_name = self._KEY_FORMAT.format(**f.metadata)
        return self._s3_bucket.Object(key_name)

    @property
    def _s3_host(self):
        h = environ.get('AWS_S3_HOST')
        if h is not None:
            return 'https://' + h
        r = environ.get('AWS_REGION') or environ.get('AWS_DEFAULT_REGION')
        if r is not None:
            return 'https://s3-' + r + '.amazonaws.com'
        else:
            return None

    @memoized_property
    def _s3(self):
        # boto3 uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        # boto3 will use AWS_DEFAULT_REGION if AWS_REGION is not set
        return boto3.resource('s3',
                              region_name=environ.get('AWS_REGION'),
                              endpoint_url=self._s3_host)

    @memoized_property
    def _s3_client(self):
        boto_session = boto3.Session()
        return boto_session.client('s3')

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

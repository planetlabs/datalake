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

from os import environ
import urlparse
from memoized_property import memoized_property
import simplejson as json
from datalake import File
from datalake_common.errors import InsufficientConfiguration
from datalake_common import Metadata
import requests

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.connection import NoHostProvided


class UnsupportedStorageError(Exception):
    pass


class DatalakeHttpError(Exception):
    pass


class Archive(object):

    def __init__(self, storage_url=None, http_url=None):
        self.storage_url = storage_url or environ.get('DATALAKE_STORAGE_URL')
        self._validate_storage_url()
        self._http_url = http_url

    def _validate_storage_url(self):
        if not self.storage_url:
            raise UnsupportedStorageError('Please specify a storage URL')

        if not self._parsed_storage_url.scheme == 's3':
            msg = 'Unsupported storage scheme ' + \
                self._parsed_storage_url.scheme
            raise UnsupportedStorageError(msg)

    @property
    def _parsed_storage_url(self):
        return urlparse.urlparse(self.storage_url)

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
        response = requests.get(url, params=params)

        while True:
            self._check_http_response(response)
            response = response.json()
            for record in response['records']:
                yield record
            if response['next']:
                response = requests.get(response['next'])
            else:
                break

    @property
    def http_url(self):
        self._http_url = self._http_url or environ.get('DATALAKE_HTTP_URL')
        if self._http_url is None:
            raise InsufficientConfiguration('Please specify DATALAKE_HTTP_URL')
        return self._http_url.rstrip('/')

    def _check_http_response(self, response):
        if response.status_code == 400:
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
        key = self._s3_key_from_metadata(f)
        key.set_metadata('datalake', json.dumps(f.metadata))
        key.set_contents_from_string(f.read())

    def url_from_file(self, f):
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
        # Note: we pass validate=False because we may just have push
        # permissions. If validate is not False, boto tries to list the
        # bucket. And this will 403.
        return self._s3_conn.get_bucket(self._s3_bucket_name, validate=False)

    _KEY_FORMAT = '{prefix}-{where}/{what}/{start}/{id}'

    def _s3_key_from_metadata(self, f):
        # For performance reasons, s3 keys should start with a short random
        # sequence:
        # https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/
        # http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html
        prefix = f.metadata['hash'][0]
        key_name = self._KEY_FORMAT.format(prefix=prefix,
                                           **f.metadata)
        return Key(self._s3_bucket, name=key_name)

    @property
    def _s3_host(self):
        r = environ.get('AWS_REGION')
        if r is not None:
            return 's3-' + r + '.amazonaws.com'
        else:
            return NoHostProvided

    @property
    def _s3_conn(self):
        if not hasattr(self, '_conn'):
            k = environ.get('AWS_ACCESS_KEY_ID')
            s = environ.get('AWS_SECRET_ACCESS_KEY')
            self._conn = S3Connection(aws_access_key_id=k,
                                      aws_secret_access_key=s,
                                      host=self._s3_host)
        return self._conn

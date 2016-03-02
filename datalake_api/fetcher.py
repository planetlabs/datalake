# Copyright 2016 Planet Labs, Inc.
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
from mimetypes import guess_type
from datalake_common import Metadata
from datalake_common.errors import NoSuchDatalakeFile
import simplejson as json
import re
from memoized_property import memoized_property
from botocore.exceptions import ClientError as BotoClienError


class ArchiveFile(object):

    def __init__(self, fd, metadata):
        self.fd = fd
        self.metadata = metadata
        self.read = self.fd.read
        self._guess_type()

    _has_trailing_checksum = re.compile(r'(?P<path>.+)-[0-9a-f]{32,40}?')

    @memoized_property
    def _adjusted_path(self):
        m = self._has_trailing_checksum.match(self.metadata['path'])
        if m:
            return m.group('path')
        return self.metadata['path']

    def _guess_type(self):
        self.content_type, self.content_encoding = \
            guess_type(self._adjusted_path)
        if self.content_type is None and self._is_log_file():
            self.content_type = 'text/plain'

    _KNOWN_LOGS = ['syslog', 'dmesg']

    def _is_log_file(self):
        return self.metadata['what'] in self._KNOWN_LOGS or \
            self._adjusted_path.endswith('.log') or \
            self._adjusted_path.endswith('.log.gz') or \
            self._adjusted_path.endswith('.log.1.gz') or \
            self._adjusted_path.endswith('.log.1')


class ArchiveFileFetcher(object):

    def __init__(self, s3_bucket):
        self.s3_bucket = s3_bucket

    def get_file(self, file_id):
        key = self._get_s3_key(file_id)
        fd = key['Body']
        j = json.loads(key['Metadata']['datalake'])
        metadata = Metadata(j)
        return ArchiveFile(fd, metadata)

    def _get_s3_key(self, file_id):
        path = '{}/data'.format(file_id)
        try:
            return self.s3_bucket.Object(path).get()
        except BotoClienError as e:
            if e.response['Error']['Code'] == "404":
                msg = 'No file with id {} exists'.format(file_id)
                raise NoSuchDatalakeFile(msg)
            else:
                raise

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
from memoized_property import memoized_property
from pyblake2 import blake2b
from translator import Translator

from datalake_common import Metadata


class File(object):
    '''A File to be manipulated by the Archive'''

    def __init__(self, path, **metadata_fields):
        '''Create a File

        Args:

            path: path to the file

            metadata_fields: known metadata fields that go with this
            file. Missing fields will be added if they can be
            determined. Othwerise, InvalidDatalakeMetadata will be raised.

        '''
        self._fd = open(path, 'r')
        self._path = os.path.abspath(path)
        self._initialize_methods_from_fd()
        self._infer_metadata_fields(metadata_fields)
        self.metadata = Metadata(metadata_fields)

    def _initialize_methods_from_fd(self):
        for m in ['read', 'readlines', 'seek', 'tell', 'close']:
            setattr(self, m, getattr(self._fd, m))

    def _infer_metadata_fields(self, metadata_fields):
        self._infer_hash(metadata_fields)
        self._infer_where(metadata_fields)
        self._apply_translations(metadata_fields)

    def _infer_hash(self, metadata_fields):
        if 'hash' not in metadata_fields:
            # do not recalculate the hash if it is already known
            metadata_fields['hash'] = self._calculate_hash()

    def _infer_where(self, metadata_fields):
        default_where = os.environ.get('DATALAKE_DEFAULT_WHERE')
        where = metadata_fields.get('where')
        if where is None and default_where is not None:
            metadata_fields['where'] = default_where

    def _apply_translations(self, metadata_fields):
        for f in ['where', 'what', 'work_id']:
            value = metadata_fields.get(f)
            if value is None or '~' not in value:
                continue
            t = Translator(value)
            metadata_fields[f] = t.translate(self._path)

    _HASH_BUF_SIZE = 65536

    def _calculate_hash(self):
        '''16-byte blake2b hash over the content of this file'''
        # this takes just under 2s on my laptop for a 1GB file.
        b2 = blake2b(digest_size=16)
        with open(self._path, 'rb') as f:
            while True:
                data = f.read(self._HASH_BUF_SIZE)
                if not data:
                    break
                b2.update(data)
        return b2.hexdigest()

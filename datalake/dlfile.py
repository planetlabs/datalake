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
from pyblake2 import blake2b
from .translator import Translator
from io import BytesIO
import tarfile
import simplejson as json
from datalake_common import Metadata


class InvalidDatalakeBundle(Exception):
    pass


class File(object):
    '''A File to be manipulated by the Archive'''

    def __init__(self, fd, **metadata_fields):
        '''Create a File

        Args:

            fd: file-like object from which the file data can be read.

            metadata_fields: known metadata fields that go with this
            file. Missing fields will be added if they can be
            determined. Othwerise, InvalidDatalakeMetadata will be raised.

        '''
        self._fd = fd
        self._initialize_methods_from_fd()
        self._infer_metadata_fields(metadata_fields)
        self.metadata = Metadata(metadata_fields)

    @classmethod
    def from_filename(cls, filename, **metadata_fields):
        '''Create a File from a filename

        This is a convenience method that opens the file and adds the filename
        to the metadata_fields for convenience.

        '''
        filename = os.path.abspath(filename)
        if 'path' not in metadata_fields:
            metadata_fields['path'] = filename
        fd = open(filename, 'rb')
        return cls(fd, **metadata_fields)

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
            metadata_fields[f] = t.translate(metadata_fields['path'])

    _HASH_BUF_SIZE = 65536

    def _calculate_hash(self):
        '''16-byte blake2b hash over the content of this file'''
        # this takes just under 2s on my laptop for a 1GB file.
        b2 = blake2b(digest_size=16)
        current = self.tell()
        self.seek(current)
        while True:
            data = self.read(self._HASH_BUF_SIZE)
            if not data:
                break
            b2.update(data)
        self.seek(current)
        return b2.hexdigest()

    # bundle file version 0 is very simple. It is a tar file with three
    # members:
    #
    # version: a single file with the single character '0'
    # content: the contents of the file to be archived
    # datalake-metadata.json: the datalake metadata as a json
    DATALAKE_BUNDLE_VERSION = '0'

    @classmethod
    def from_bundle(cls, bundle_filename):
        '''Create a File from a bundle

        What's a bundle? It's the file and the metadata together in a single
        file. This is used for passing around a file after it's metadata has
        been prepared, but before it has been uploaded to the datalake.

        '''
        cls._validate_bundle(bundle_filename)
        b = tarfile.open(bundle_filename, 'r:')
        cls._validate_bundle_version(b)
        m = cls._get_metadata_from_bundle(b)
        c = cls._get_fd_from_bundle(b, 'content')
        f = cls(c, **m)

        return f

    @staticmethod
    def _validate_bundle(bundle_filename):
        if not tarfile.is_tarfile(bundle_filename):
            msg = '{} is not a valid bundle file (not a tar)'
            raise InvalidDatalakeBundle(msg.format(bundle_filename))

    @staticmethod
    def _validate_bundle_version(bundle):
        v = File._get_content_from_bundle(bundle, 'version').decode('utf-8')
        if v != File.DATALAKE_BUNDLE_VERSION:
            msg = '{} has unsupported bundle version {}.'
            msg = msg.format(bundle.name, v)
            raise InvalidDatalakeBundle(msg)

    @staticmethod
    def _get_metadata_from_bundle(b):
        try:
            m = File._get_fd_from_bundle(b, 'datalake-metadata.json')
            return json.load(m)
        except json.JSONDecodeError:
            msg = "{}'s datalake-metadata.json is not a valid json"
            msg = msg.format(b.name)
            raise InvalidDatalakeBundle(msg)

    @staticmethod
    def _get_content_from_bundle(t, arcname):
        fd = File._get_fd_from_bundle(t, arcname)
        return fd.read()

    @staticmethod
    def _get_fd_from_bundle(t, arcname):
        try:
            fd = t.extractfile(arcname)
            if fd is None:
                raise KeyError()
            return fd
        except KeyError:
            msg = '{} has no {}.'.format(t.name, arcname)
            raise InvalidDatalakeBundle(msg)

    def to_bundle(self, bundle_filename):
        '''write file bundled with its metadata

        Args:
        bundle_filename: output file
        '''
        t = tarfile.open(bundle_filename, 'w')
        self._add_fd_to_tar(t, 'content', self._fd)
        self._add_string_to_tar(t, 'version', self.DATALAKE_BUNDLE_VERSION)
        self._add_string_to_tar(t, 'datalake-metadata.json',
                                self.metadata.json)
        t.close()

        # reset the file pointer in case somebody else wants to read us.
        self.seek(0, 0)

    def _add_string_to_tar(self, tfile, arcname, data):
        s = BytesIO(data.encode('utf-8'))
        info = tarfile.TarInfo(name=arcname)
        s.seek(0, os.SEEK_END)
        info.size = s.tell()
        s.seek(0, 0)
        tfile.addfile(tarinfo=info, fileobj=s)

    def _add_fd_to_tar(self, tfile, arcname, fd):
        info = tarfile.TarInfo(name=arcname)
        info.size = os.fstat(fd.fileno()).st_size
        tfile.addfile(tarinfo=info, fileobj=fd)

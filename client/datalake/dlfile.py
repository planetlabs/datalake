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
from .common import Metadata
try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO
from gzip import GzipFile

ITER_SIZE = 1024 * 8


class InvalidDatalakeBundle(Exception):
    pass


class StreamingFile(object):

    '''A StreamingFile to be fetched by the Archive'''

    def __init__(self, stream, **metadata_fields):
        '''Create a StreamingFile
        A StreamingFile is never loaded as a whole into memory.

        Args:

            stream: a generator from which the file data can be read.

            metadata_fields: known metadata fields that go with this
            file. Missing fields will be added if they can be
            determined. Othwerise, InvalidDatalakeMetadata will be raised.

        '''
        self._stream = stream
        self._buffer = b''
        self._content_gen = False
        self.metadata = Metadata(metadata_fields)

    @property
    def encoding(self):
        return self._stream.encoding

    def iter_content(self):
        """Iterates over the stream of bytes.
        When stream=True is set on the fetch, the entire file is not loaded
        into memory, and is read in batches instead.
        """
        if self._stream is None:
            raise ValueError("I/O operation on closed stream")

        for chunk in self._stream:
            yield chunk

    def read(self, size=None):
        """Iterates over the stream returning the number of requested bytes.
        This avoids loading the entire file into memory at once.
        """
        if not self._content_gen:
            self._content_gen = self.iter_content()

        while size is None or len(self._buffer) < size:
            try:
                self._buffer += next(self._content_gen)
            except StopIteration:
                # buffer < size, at the end of the stream
                # so clear out buffer and return it
                ret = self._buffer
                self._buffer = b''
                return ret

        ret, self._buffer = (self._buffer[:size], self._buffer[size:])
        return ret

    def readlines(self):
        """Iterates over the stream, one line at a time.
        this avoids reading the entire file into memory at once.
        """

        pending = None

        for chunk in self.iter_content():

            if pending is not None:
                chunk = pending + chunk

            lines = chunk.splitlines(True)

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield line

        if pending is not None:
            yield pending

    def close(self):
        if self._stream is not None:
            self._stream.close()
        self._stream = None
        self._buffer = b''
        self._content_gen = False


class StreamingHTTPFile(StreamingFile):

    '''A StreamingHTTPFile to be fetched by the Archive
    Optimized with larger chunk size for large file delivery over HTTP
    '''

    def iter_content(self, chunk_size=ITER_SIZE):
        return self._stream.iter_content(chunk_size)


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

    @classmethod
    def from_filename_compressed(cls, filename, **metadata_fields):
        '''Create a File from a filename, compressing the input data first

        This is a convenience method that will create a compressed File from
        the specified filename.
        '''
        filename = os.path.abspath(filename)
        if 'path' not in metadata_fields:
            metadata_fields['path'] = filename

        fgz = StringIO()
        gz = GzipFile(filename=filename, mode='wb', fileobj=fgz)
        with gz, open(filename, 'rb') as f:
            gz.write(f.read())

        fgz.seek(0, os.SEEK_SET)

        return cls(fgz, **metadata_fields)

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
            msg = '{} has unsupported bundle version {}'
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
        temp_filename = self._dot_filename(bundle_filename)
        with open(temp_filename, 'wb') as f:
            t = tarfile.open(fileobj=f, mode='w')
            self._add_fd_to_tar(t, 'content', self._fd)
            self._add_string_to_tar(t, 'version', self.DATALAKE_BUNDLE_VERSION)
            self._add_string_to_tar(t, 'datalake-metadata.json',
                                    self.metadata.json)
        os.rename(temp_filename, bundle_filename)

        # reset the file pointer in case somebody else wants to read us.
        self.seek(0, 0)

    def _dot_filename(self, path):
        return os.path.join(os.path.dirname(path),
                            '.{}'.format(os.path.basename(path)))

    def _add_string_to_tar(self, tfile, arcname, data):
        s = BytesIO(data.encode('utf-8'))
        info = tarfile.TarInfo(name=arcname)
        s.seek(0, os.SEEK_END)
        info.size = s.tell()
        s.seek(0, 0)
        tfile.addfile(tarinfo=info, fileobj=s)

    def _add_fd_to_tar(self, tfile, arcname, fd):
        info = tarfile.TarInfo(name=arcname)
        info.size = self._get_fd_size(fd)
        tfile.addfile(tarinfo=info, fileobj=fd)

    def _get_fd_size(self, fd):
        p = fd.tell()
        fd.seek(0, os.SEEK_END)
        size = fd.tell()
        fd.seek(p, os.SEEK_SET)
        return size

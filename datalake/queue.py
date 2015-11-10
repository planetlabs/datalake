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

'''manage a queue of datalake files

This allows users to enqueue files to be uploaded to the datalake. An uploader
process runs that actually does the uploader work.

Under the hood, the queue is a directory which the Uploader watches. The
Enqueuer enqueues files by setting an extended filesystem attribute with the
fully-formed metadata for the file to be uploaded, and symlinking it to the
queue directory. This ensures that the enqueuer fails in the user's face
instead of silently behind the user's back. The uploader uses inotify to
monitor the queue directory. When a file arrives, it gets uploaded and the
symlink deleted.

'''
from os import environ
from datalake_common.errors import InsufficientConfiguration
from xattr import setxattr, getxattr
import pyinotify
from logging import getLogger
import os
import time
import shutil

from datalake_common import Metadata
from datalake import File


log = getLogger('datalake-queue')


DATALAKE_METADATA_XATTR = 'user.datalake-metadata'


class DatalakeQueueBase(object):

    def __init__(self, queue_dir=None):        
        self.queue_dir = queue_dir or environ.get('DATALAKE_QUEUE_DIR')
        self._validate_queue_dir()

    def _validate_queue_dir(self):
        if self.queue_dir is None:
            raise InsufficientConfiguration('Please specify a queue directory')
        self.queue_dir = os.path.abspath(self.queue_dir)


class Enqueuer(DatalakeQueueBase):

    def enqueue(self, path, **metadata_fields):
        '''enqueue a file with the specified metadata o be pushed

        Returns the File with complete metadata that will be pushed.
        '''
        log.info('Enqueing ' + path)
        f = File(path, **metadata_fields)
        setxattr(path, DATALAKE_METADATA_XATTR, f.metadata.json)
        dest = os.path.join(self.queue_dir, f.metadata['id'])
        os.symlink(path, dest)        
        return f


class Uploader(DatalakeQueueBase):

    def __init__(self, archive, queue_dir):
        super(Uploader, self).__init__(queue_dir)
        self._archive = archive

    class EventHandler(pyinotify.ProcessEvent):

        def __init__(self, callback):
            super(Uploader.EventHandler, self).__init__()
            self.callback = callback

        def process_IN_CREATE(self, event):
            self.callback(event.pathname)

    def _setup_watch_manager(self, timeout):
        if timeout is not None:
            timeout = int(timeout * 1000)
        self._wm = pyinotify.WatchManager()
        self._handler = Uploader.EventHandler(self._push)
        self._notifier = pyinotify.Notifier(self._wm, self._handler,
                                            timeout=timeout)
        self._wm.add_watch(self.queue_dir, pyinotify.IN_CREATE)

    def _push(self, path):
        metadata = Metadata.from_json(getxattr(path, DATALAKE_METADATA_XATTR))
        f = File(path, **metadata)
        url = self._archive.push(f)
        log.info('Pushed ' +  path + ' to ' + url)
        os.unlink(path)

    def listen(self, timeout=None):
        '''listen for files in the queue directory and push them'''
        for f in os.listdir(self.queue_dir):
            path = os.path.join(self.queue_dir, f)
            self._push(path)

        self._run(timeout)

    INFINITY = None

    def _run(self, timeout):

        self._prepare_to_track_run_time(timeout)
        self._notifier.process_events()
        while self._notifier.check_events():
            self._notifier.read_events()
            self._notifier.process_events()
            if self._update_time_remaining() == 0:
                break

    def _update_time_remaining(self):
        if self._run_time_remaining is self.INFINITY:
            return self.INFINITY
        now = time.time()
        duration = now - self._run_start
        self._run_time_remaining -= duration
        self._run_time_remaining = max(self._run_time_remaining, 0)
        self._run_start = now
        return self._run_time_remaining

    def _prepare_to_track_run_time(self, timeout):
        self._setup_watch_manager(timeout)
        self._run_start = time.time()
        self._run_time_remaining = timeout

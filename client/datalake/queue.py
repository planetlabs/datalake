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
Enqueuer enqueues files by tarring them up with their metadata and writing them
to the queue directory. This ensures that the enqueuer fails in the user's face
instead of silently behind the user's back if the metadata is not right. The
uploader uses inotify to monitor the queue directory. When a file arrives, it
gets uploaded. On success, it gets deleted. If the upload fails for some
reason, the file remains in the queue.
'''
from os import environ
import os
from .common.errors import InsufficientConfiguration
from logging import getLogger
import time
from threading import Thread
from six.moves._thread import interrupt_main
from six.moves.queue import Queue

from datalake import File, InvalidDatalakeBundle


'''whether or not queue feature is available

Users may wish to check if s3 features are available before invoking them. If
they are unavailable, the affected functions will raise
InsufficientConfiguration.'''
has_queue = True
try:
    import inotify_simple
except ImportError:
    has_queue = False


def requires_queue(f):
    def wrapped(*args, **kwargs):
        if not has_queue:
            msg = 'This feature requires the queuable deps.  '
            msg += '`pip install datalake[queuable]` to turn this feature on.'
            raise InsufficientConfiguration(msg)
        return f(*args, **kwargs)
    return wrapped


log = getLogger('datalake-queue')


class DatalakeQueueBase(object):

    @requires_queue
    def __init__(self, queue_dir=None):
        self.queue_dir = queue_dir or environ.get('DATALAKE_QUEUE_DIR')
        self._validate_queue_dir()

    def _validate_queue_dir(self):
        if self.queue_dir is None:
            raise InsufficientConfiguration('Please set DATALAKE_QUEUE_DIR')
        self.queue_dir = os.path.abspath(self.queue_dir)


class Enqueuer(DatalakeQueueBase):

    def enqueue(self, filename, compress=False, **metadata_fields):
        '''enqueue a file with the specified metadata to be pushed

        Args:
            filename: the file to enqueue

            compress: whether or not to compress the file before enqueueing

        Returns the File with complete metadata that will be pushed.

        '''
        log.info('Enqueing ' + filename)
        if compress:
            try:
                f = File.from_filename_compressed(filename, **metadata_fields)
            except OverflowError:
                log.warning('Compression failed. Falling back to uncompressed '
                            'uploads')
                f = File.from_filename(filename, **metadata_fields)
        else:
            f = File.from_filename(filename, **metadata_fields)
        fname = f.metadata['id'] + '.tar'
        dest = os.path.join(self.queue_dir, fname)
        f.to_bundle(dest)
        return f


class Uploader(DatalakeQueueBase):

    def __init__(self, archive, queue_dir, callback=None):
        '''create an uploader that listens to queue_dir and pushes to archive

        The callback (if any) gets called with the filename after each
        successful upload. Note that it may be called from a thread. So be safe
        out there.
        '''
        super(Uploader, self).__init__(queue_dir)
        self._archive = archive
        self._callback = callback

        self.inotify = inotify_simple.INotify()

    def _setup_watch_manager(self, timeout):
        flags = inotify_simple.flags
        watch_flags = flags.CLOSE_WRITE | flags.MOVED_TO
        self.inotify.add_watch(self.queue_dir, watch_flags)

    def _push(self, filename):
        if not os.path.isabs(filename):
            filename = os.path.join(self.queue_dir, filename)
        if os.path.basename(filename).startswith('.'):
            return
        if not self._workers:
            self._synchronous_push(filename)
        else:
            self._threaded_push(filename)

    def _synchronous_push(self, filename):
        try:
            f = File.from_bundle(filename)
        except InvalidDatalakeBundle as e:
            msg = '{}. Skipping upload.'.format(e.args[0])
            log.exception(msg)
            return
        url = self._archive.push(f)
        msg = 'Pushed {}({}) to {}'.format(filename, f.metadata['path'], url)
        log.info(msg)
        os.unlink(filename)
        if self._callback is not None:
            self._callback(filename)

    def _threaded_push(self, filename):
        self._queue.put(filename)

    def _threaded_worker(self, worker_number):
        log.info('upload worker {} starting.'.format(worker_number))
        try:
            while True:
                filename = self._queue.get(block=True)
                msg = 'upload worker {} handling {}'
                msg = msg.format(worker_number, filename)
                log.info(msg)
                self._synchronous_push(filename)
                self._queue.task_done()
        except Exception as e:
            log.exception(e)
            # when a worker fails, we fail the entire process.
            interrupt_main()

    def listen(self, timeout=None, workers=1):
        try:
            self._listen(timeout=timeout, workers=workers)
        except Exception as e:
            log.exception(e)
            raise

    def _listen(self, timeout=None, workers=1):
        '''listen for files in the queue directory and push them'''
        from . import __version__

        log.info('------------------------------')
        log.info('datalake ' + __version__)

        self._workers = []
        if workers <= 0:
            msg = 'number of upload workers cannot be zero or negative'
            raise InsufficientConfiguration(msg)
        if workers > 1:
            # when multiple workers are requested, the main thread monitors the
            # queue directory and puts the files in a Queue that is serviced by
            # the worker threads. So the word queue is a bit overloaded in this
            # module.
            self._queue = Queue()
            self._workers = [self._create_worker(i) for i in range(workers)]

        for f in os.listdir(self.queue_dir):
            path = os.path.join(self.queue_dir, f)
            self._push(path)

        self._run(timeout)

    def _create_worker(self, worker_number):
        w = Thread(target=self._threaded_worker, args=(worker_number,))
        w.setDaemon(True)
        w.start()
        return w

    INFINITY = None

    def _run(self, timeout):

        self._prepare_to_track_run_time(timeout)
        if timeout is not None:
            timeout = int(timeout * 1000)

        while self._update_time_remaining() > 0:
            for event in self.inotify.read(timeout=timeout):
                if event.name is None:
                    continue
                self._push(event.name)
                if self._update_time_remaining() == 0:
                    break

    def _update_time_remaining(self):
        if self._run_time_remaining is self.INFINITY:
            return float('inf')
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

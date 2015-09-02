from memoized_property import memoized_property
from record import DatalakeRecord
from errors import InsufficientConfiguration
from queue import SQSQueue
from translator import S3ToDatalakeTranslator


class Ingester(object):

    def __init__(self, storage, queue_name=None):
        self.storage = storage
        self.queue_name = queue_name

    def ingest(self, url):
        '''ingest the metadata associated with the given url'''
        records = DatalakeRecord.list_from_url(url)
        for r in records:
            self.storage.store(r)

    @memoized_property
    def _queue(self):
        if not self.queue_name:
            raise InsufficientConfiguration('No queue configured.')
        return SQSQueue(self.queue_name, self.handler)

    @memoized_property
    def _translator(self):
        return S3ToDatalakeTranslator()

    def handler(self, msg):
        records = self._translator.translate(msg)
        for r in records:
            self.storage.store(r)

    def listen(self, timeout=None):
        '''listen to the queue, ingest what you hear, and report'''
        self._queue.drain(timeout=timeout)

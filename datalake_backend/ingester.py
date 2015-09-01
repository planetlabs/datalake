from record import DatalakeRecord

class Ingester(object):

    def __init__(self, storage):
        self.storage = storage

    def ingest(self, url):
        '''ingest the metadata associated with the given url'''
        records = DatalakeRecord.list_from_url(url)
        for r in records:
            self.storage.store(r)

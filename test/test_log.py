from unittest import TestCase

from atl import Log, InvalidATLMetadata, UnsupportedATLMetadataVersion


class TestFromATLMetadata(TestCase):

    def setUp(self):
        self.metadata = {
            'version': '0',
            'start': '2015-03-20T00:00:00Z',
            'end': '2015-03-20T23:59:59.999Z',
            'where': 'nebraska',
            'what': 'apache',
        }

    def test_missing_version(self):
        del(self.metadata['version'])
        with self.assertRaises(InvalidATLMetadata):
            Log.from_atl_metadata(self.metadata)

    def test_unsupported_version(self):
        self.metadata['version'] = '100'
        with self.assertRaises(UnsupportedATLMetadataVersion):
            Log.from_atl_metadata(self.metadata)

    def test_normalize_date(self):
        self.metadata['start'] = '2015-03-20'
        l = Log.from_atl_metadata(self.metadata)
        self.assertEqual(l.metadata['start'], '2015-03-20T00:00:00Z')

    def test_invalid_date(self):
        self.metadata['end'] = 'bxfl230'
        with self.assertRaises(InvalidATLMetadata):
            Log.from_atl_metadata(self.metadata)

from unittest import TestCase

from atl import Metadata, InvalidATLMetadata, UnsupportedATLMetadataVersion


class TestMetadataValidation(TestCase):

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
            Metadata(self.metadata)

    def test_unsupported_version(self):
        self.metadata['version'] = '100'
        with self.assertRaises(UnsupportedATLMetadataVersion):
            Metadata(self.metadata)

    def test_normalize_date(self):
        self.metadata['start'] = '2015-03-20'
        m = Metadata(self.metadata)
        self.assertEqual(m['start'], '2015-03-20T00:00:00Z')

    def test_invalid_date(self):
        self.metadata['end'] = 'bxfl230'
        with self.assertRaises(InvalidATLMetadata):
            Metadata(self.metadata)

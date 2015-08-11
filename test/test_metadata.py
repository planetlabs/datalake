from unittest import TestCase

from datalake import Metadata, InvalidDatalakeMetadata, \
    UnsupportedDatalakeMetadataVersion


class TestMetadataValidation(TestCase):

    def setUp(self):
        self.metadata = {
            'version': '0',
            'start': 1426809600000,
            'end': 1426895999999,
            'where': 'nebraska',
            'what': 'apache',
        }

    def test_missing_version(self):
        del(self.metadata['version'])
        with self.assertRaises(InvalidDatalakeMetadata):
            Metadata(self.metadata)

    def test_unsupported_version(self):
        self.metadata['version'] = '100'
        with self.assertRaises(UnsupportedDatalakeMetadataVersion):
            Metadata(self.metadata)

    def test_normalize_date(self):
        self.metadata['start'] = '2015-03-20'
        m = Metadata(self.metadata)
        self.assertEqual(m['start'], 1426809600000)

    def test_invalid_date(self):
        self.metadata['end'] = 'bxfl230'
        with self.assertRaises(InvalidDatalakeMetadata):
            Metadata(self.metadata)

from unittest import TestCase

from datalake import Metadata, InvalidDatalakeMetadata, \
    UnsupportedDatalakeMetadataVersion


class TestMetadataValidation(TestCase):

    def setUp(self):
        self.metadata = {
            'version': 0,
            'start': 1426809600000,
            'end': 1426895999999,
            'where': 'nebraska',
            'what': 'apache',
        }

    def test_version_default(self):
        del(self.metadata['version'])
        m = Metadata(self.metadata)
        assert 'version' in m
        assert m['version'] == 0

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

    def test_id_gets_assigned(self):
        m = Metadata(self.metadata)
        assert 'id' in m
        assert m['id'] is not None

    def test_none_for_required_field(self):
        self.metadata['where'] = None
        with self.assertRaises(InvalidDatalakeMetadata):
            Metadata(self.metadata)

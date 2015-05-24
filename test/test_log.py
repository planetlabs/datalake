from unittest import TestCase

from atl import Log, InvalidATLMetadata, UnsupportedATLMetadataVersion


class TestFromATLMetadata(TestCase):

    def test_missing_version(self):
        m = {
            'start': '2015-03-20T00:00:00Z',
            'end': '2015-03-20T23:59:59Z',
            'where': 'nebraska',
            'what': 'apache',
        }
        with self.assertRaises(InvalidATLMetadata):
            Log.from_atl_metadata(m)

    def test_unsupported_version(self):
        m = {
            'version': '100',
            'start': '2015-03-20T00:00:00Z',
            'end': '2015-03-20T23:59:59Z',
            'where': 'nebraska',
            'what': 'apache',
        }
        with self.assertRaises(UnsupportedATLMetadataVersion):
            Log.from_atl_metadata(m)

import pytest
from . import basic_metadata

from datalake import Metadata, InvalidDatalakeMetadata, \
    UnsupportedDatalakeMetadataVersion


def test_version_default(basic_metadata):
    del(basic_metadata['version'])
    m = Metadata(basic_metadata)
    assert 'version' in m
    assert m['version'] == 0

def test_unsupported_version(basic_metadata):
    basic_metadata['version'] = '100'
    with pytest.raises(UnsupportedDatalakeMetadataVersion):
        Metadata(basic_metadata)

def test_normalize_date(basic_metadata):
    basic_metadata['start'] = '2015-03-20'
    m = Metadata(basic_metadata)
    assert m['start'] == 1426809600000

def test_invalid_date(basic_metadata):
    basic_metadata['end'] = 'bxfl230'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_id_gets_assigned(basic_metadata):
    m = Metadata(basic_metadata)
    assert 'id' in m
    assert m['id'] is not None

def test_none_for_required_field(basic_metadata):
    basic_metadata['where'] = None
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_work_id_gets_assigned(basic_metadata):
    m = Metadata(basic_metadata)
    assert 'work_id' in m
    assert m['work_id'] is None

def test_id_not_overwritten(basic_metadata):
    basic_metadata['id'] = '123'
    m = Metadata(basic_metadata)
    assert 'id' in m
    assert m['id'] == '123'

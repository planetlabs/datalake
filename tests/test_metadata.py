import pytest
import simplejson as json

from datalake_common import Metadata, InvalidDatalakeMetadata, \
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

def test_no_end_allowed(basic_metadata):
    del(basic_metadata['end'])
    m = Metadata(basic_metadata)
    assert 'end' not in m

def test_unallowed_characters(basic_metadata):
    basic_metadata['what'] = '123#$'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_unallowed_capitals(basic_metadata):
    basic_metadata['what'] = 'MYFILE'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_unallowed_spaces(basic_metadata):
    basic_metadata['where'] = 'SAN FRANCISCO'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_unallowed_dots(basic_metadata):
    basic_metadata['where'] = 'this.that.com'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_work_id_null_string_unallowed(basic_metadata):
    basic_metadata['work_id'] = 'null'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_work_id_with_unallowed_characters(basic_metadata):
    basic_metadata['work_id'] = 'foojob#123'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

basic_json = ('{"start": 1426809600000, "what": "apache", "version": 0, '
              '"end": 1426895999999, "hash": "12345", "where": "nebraska", '
              '"id": "9f8f8b618f48424c8d69a7ed76c88f05", "work_id": null}')

def test_from_to_json(basic_metadata):
    m1 = Metadata.from_json(basic_json)
    m2 = m1.json
    assert sorted(m2) == sorted(basic_json)

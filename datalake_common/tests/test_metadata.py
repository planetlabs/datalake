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
              '"id": "9f8f8b618f48424c8d69a7ed76c88f05", "work_id": null, '
              '"data_version": "1"}')

def test_from_to_json(basic_metadata):
    m1 = Metadata.from_json(basic_json)
    m2 = m1.json
    assert sorted(m2) == sorted(basic_json)

def test_end_before_start(basic_metadata):
    end = basic_metadata['end']
    basic_metadata['end'] = basic_metadata['start']
    basic_metadata['start'] = end
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_integer_data_version(basic_metadata):
    basic_metadata['data_version'] = 1
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_none_data_version(basic_metadata):
    basic_metadata['data_version'] = None
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_missing_data_version(basic_metadata):
    del(basic_metadata['data_version'])
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

def test_random_metadata(random_metadata):
    # Others rely on datalake-common's random_metadata to be valid. So make
    # sure it doesn't throw any errors.
    Metadata(random_metadata)

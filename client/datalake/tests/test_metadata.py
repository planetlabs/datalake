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
from dateutil.parser import parse as dateparse

from datalake.common import Metadata, InvalidDatalakeMetadata, \
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
    assert m['end'] is None


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
              '"path": "/var/log/apache/access.log.1"}')


def test_from_to_json(basic_metadata):
    m1 = Metadata.from_json(basic_json)
    m2 = m1.json
    assert sorted(m2) == sorted(basic_json)


def test_from_invalid_json():
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata.from_json('{flee floo')


def test_from_none_json():
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata.from_json(None)


def test_end_before_start(basic_metadata):
    end = basic_metadata['end']
    basic_metadata['end'] = basic_metadata['start']
    basic_metadata['start'] = end
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)


def test_random_metadata(random_metadata):
    # Others rely on datalake-common's random_metadata to be valid. So make
    # sure it doesn't throw any errors.
    Metadata(random_metadata)


def test_normalize_float_date(basic_metadata):
    basic_metadata['start'] = '1426809600.123'
    m = Metadata(basic_metadata)
    assert m['start'] == 1426809600123


def test_normalize_int_date(basic_metadata):
    basic_metadata['end'] = '1426809600123'
    m = Metadata(basic_metadata)
    assert m['end'] == 1426809600123


def test_normalize_date_with_datetime(basic_metadata):
    date = dateparse('2015-03-20T00:00:00Z')
    ms = Metadata.normalize_date(date)
    assert ms == 1426809600000


def test_normalize_garbage(basic_metadata):
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata.normalize_date('bleeblaaablooo')


def test_path_with_leading_dot_not_allowed(basic_metadata):
    basic_metadata['path'] = './abc.txt'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)


def test_relative_path_not_allowed(basic_metadata):
    basic_metadata['path'] = 'abc.txt'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)


def test_absolute_windows_path(basic_metadata):
    path = r'Z:\\foo\bar.txt'
    basic_metadata['path'] = path
    m = Metadata(basic_metadata)
    assert m['path'] == path


def test_absolute_windows_path_single_slash(basic_metadata):
    # some cygwin environments seem to have a single slash after the
    # drive. Shrug.
    path = r'Z:\foo\bar.txt'
    basic_metadata['path'] = path
    m = Metadata(basic_metadata)
    assert m['path'] == path


def test_relative_windows_path_not_allowed(basic_metadata):
    basic_metadata['path'] = r'foo\abc.txt'
    with pytest.raises(InvalidDatalakeMetadata):
        Metadata(basic_metadata)

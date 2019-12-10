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

from datalake.common import has_s3, DatalakeRecord, InvalidDatalakeMetadata
from datalake.common.errors import InsufficientConfiguration, \
    UnsupportedTimeRange, NoSuchDatalakeFile
import time


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_list_from_s3_url(s3_file_from_metadata, random_metadata):
    url = 's3://foo/bar'
    s3_file_from_metadata(url, random_metadata)
    records = DatalakeRecord.list_from_url(url)
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == random_metadata


@pytest.mark.skipif(has_s3, reason='')
def test_from_url_fails_without_boto():
    with pytest.raises(InsufficientConfiguration):
        DatalakeRecord.list_from_url('s3://foo/bar')


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_list_from_metadata(s3_file_from_metadata, random_metadata):
    url = 's3://foo/baz'
    s3_file_from_metadata(url, random_metadata)
    records = DatalakeRecord.list_from_metadata(url, random_metadata)
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == random_metadata


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_timespan_too_big(s3_file_from_metadata, random_metadata):
    url = 's3://foo/blapp'
    s3_file_from_metadata(url, random_metadata)
    random_metadata['start'] = 0
    random_metadata['end'] = (DatalakeRecord.MAXIMUM_BUCKET_SPAN + 1) * \
        DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    with pytest.raises(UnsupportedTimeRange):
        DatalakeRecord.list_from_metadata(url, random_metadata)


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_no_such_datalake_file_in_bucket(s3_bucket_maker):
    s3_bucket_maker('test-bucket')
    url = 's3://test-bucket/such/file'
    with pytest.raises(NoSuchDatalakeFile):
        DatalakeRecord.list_from_url(url)


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_no_such_bucket(s3_connection):
    url = 's3://no/such/file'
    with pytest.raises(NoSuchDatalakeFile):
        DatalakeRecord.list_from_url(url)


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_no_end(random_metadata, s3_file_from_metadata):
    url = 's3://foo/baz'
    del(random_metadata['end'])
    expected_metadata = random_metadata.copy()
    expected_metadata['end'] = None
    s3_file_from_metadata(url, random_metadata)
    records = DatalakeRecord.list_from_metadata(url, random_metadata)
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == expected_metadata


def test_get_time_buckets_misaligned():
    # Test for regression on bug when querying over x buckets for a timeframe
    # (end - start) of < x buckets (i.e. end of B0 to start of B2)
    start = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS * 4 / 5
    end = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS * 11 / 5
    buckets = DatalakeRecord.get_time_buckets(start, end)
    assert buckets == [0, 1, 2]


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_no_metadata(s3_file_maker):
    url = 's3://foo/bar'
    s3_file_maker('foo', 'bar', 'the content', None)
    with pytest.raises(InvalidDatalakeMetadata):
        DatalakeRecord.list_from_url(url)


@pytest.mark.skipif(not has_s3, reason='requires s3 features')
def test_record_size_and_create_time(s3_file_maker, random_metadata):
    url = 's3://foo/bar'
    now = int(time.time() * 1000.0)

    # s3 create times have a 1s resolution. So we just tolerate 2x that to
    # ensure the test passes reasonably.
    max_tolerable_delta = 2000

    s3_file_maker('foo', 'bar', 'thissongisjust23byteslong', random_metadata)
    records = DatalakeRecord.list_from_url(url)
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == random_metadata
        assert abs(r['create_time'] - now) <= max_tolerable_delta
        assert r['size'] == 25

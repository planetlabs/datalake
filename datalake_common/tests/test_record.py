import pytest

from datalake_common import has_s3, DatalakeRecord
from datalake_common.errors import InsufficientConfiguration


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

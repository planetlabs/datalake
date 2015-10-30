import pytest
from tempfile import NamedTemporaryFile
import simplejson as json
from datalake_common.tests import random_metadata, tmpfile


def test_push_file(archive, random_metadata, tmpfile, s3_key):
    expected_content = 'mwahaha'
    f = tmpfile(expected_content)
    url = archive.prepare_metadata_and_push(f, **random_metadata)
    from_s3 = s3_key(url)
    assert from_s3.get_contents_as_string() == expected_content
    metadata = from_s3.get_metadata('datalake')
    assert metadata is not None
    metadata = json.loads(metadata)
    common_keys = set(metadata.keys()).intersection(random_metadata.keys())
    assert common_keys == set(random_metadata.keys())

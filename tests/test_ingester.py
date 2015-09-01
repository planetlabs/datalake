import pytest
from datalake_common.tests import random_metadata

from datalake_backend import DynamoDBStorage, Ingester


@pytest.fixture
def storage(dynamodb_records_table, dynamodb_connection):
    return DynamoDBStorage(table_name='records',
                           connection=dynamodb_connection)


@pytest.fixture
def ingester(storage):
    return Ingester(storage)


@pytest.fixture
def random_s3_file_maker(s3_file_from_record, random_metadata):
    def maker():
        url = 's3://foo/' + random_metadata['id']
        s3_file_from_record(url, random_metadata)
        return url, random_metadata
    return maker


def test_ingest_one(ingester, dynamodb_records_table, random_s3_file_maker):
    url, metadata = random_s3_file_maker()
    ingester.ingest(url)
    records = [dict(r) for r in dynamodb_records_table.scan()]
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == metadata

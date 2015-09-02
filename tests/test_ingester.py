import pytest
from datalake_common.tests import random_metadata

from conftest import all_s3_notification_specs

from datalake_backend import DynamoDBStorage, Ingester, \
    InsufficientConfiguration


@pytest.fixture
def storage(dynamodb_records_table, dynamodb_connection):
    return DynamoDBStorage(table_name='records',
                           connection=dynamodb_connection)


@pytest.fixture
def random_s3_file_maker(s3_file_from_record, random_metadata):
    def maker():
        url = 's3://foo/' + random_metadata['id']
        s3_file_from_record(url, random_metadata)
        return url, random_metadata
    return maker


def test_ingest_random(storage, dynamodb_records_table, random_s3_file_maker):
    url, metadata = random_s3_file_maker()
    ingester = Ingester(storage)
    ingester.ingest(url)
    records = [dict(r) for r in dynamodb_records_table.scan()]
    assert len(records) >= 1
    for r in records:
        assert r['metadata'] == metadata


def test_listen_no_queue(storage):
    ingester = Ingester(storage)
    with pytest.raises(InsufficientConfiguration):
        ingester.listen(timeout=1)


@pytest.fixture
def ingester_with_queue(storage, sqs_queue):
    return Ingester(storage, queue_name=sqs_queue.name)


@pytest.fixture
def records_comparator(dynamodb_records_table):

    def comparator(expected_records):
        records = [dict(r) for r in dynamodb_records_table.scan()]
        assert sorted(records) == sorted(expected_records)

    return comparator


@pytest.fixture(params=all_s3_notification_specs)
def ingestion_listener_tester(request, ingester_with_queue, spec_maker,
                              records_comparator, sqs_sender):
    spec = spec_maker(request.param)
    sqs_sender(spec['s3_notification'])
    ingester_with_queue.listen(timeout=1)
    records_comparator(spec['expected_datalake_records'])


def test_ingestion_listener_tests(ingestion_listener_tester):
    pass

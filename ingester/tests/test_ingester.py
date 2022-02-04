import pytest
import time
import logging

import os
import decimal
from unittest import mock

from datalake_ingester import DynamoDBStorage, Ingester, \
    SQSQueue, SNSReporter
from datalake.common.errors import InsufficientConfiguration


@pytest.fixture
def storage(dynamodb_records_table):
    return DynamoDBStorage(table_name='records')


@pytest.fixture
def random_s3_file_maker(s3_file_from_metadata, random_metadata):
    def maker():
        url = 's3://foo/' + random_metadata['id']
        s3_file_from_metadata(url, random_metadata)
        return url, random_metadata
    return maker


@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_ingest_random(storage, dynamodb_records_table, random_s3_file_maker):
    url, metadata = random_s3_file_maker()
    ingester = Ingester(storage)
    ingester.ingest(url)
    scan_r = dynamodb_records_table.scan()
    assert scan_r['Count'] >= 1
    for r in scan_r['Items']:
        assert r['metadata'] == metadata


@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_ingest_no_end(storage, dynamodb_records_table, s3_file_from_metadata,
                       random_metadata):
    del(random_metadata['end'])
    url = 's3://foo/' + random_metadata['id']
    s3_file_from_metadata(url, random_metadata)
    ingester = Ingester(storage)
    ingester.ingest(url)

    scan_r = dynamodb_records_table.scan()
    assert scan_r['Count'] >= 1

    # we expect a null end key to come back when the user leaves it out.
    random_metadata['end'] = None
    for r in scan_r['Items']:
        assert r['metadata'] == random_metadata


@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_listen_no_queue(storage):
    ingester = Ingester(storage)
    with pytest.raises(InsufficientConfiguration):
        ingester.listen(timeout=1)

# boto3 Dynamodb returns numeric records as Decimal() type
def replace_decimals(obj):
    if isinstance(obj, list):
        return [replace_decimals(e) for e in obj]
    elif isinstance(obj, dict):
        return {k:replace_decimals(v) for k,v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

@pytest.fixture
def records_comparator(dynamodb_records_table):

    def comparator(expected_records):
        records = dynamodb_records_table.scan()['Items']

        # Create time will be "now", so remove it before comparing
        got = [{k: v for k, v in d.items() if k != 'create_time'} for d in records]
        expected = [{k: v for k, v in d.items() if k != 'create_time'} for d in expected_records]

        print(list(expected))
        assert sorted(replace_decimals(got), key=lambda x: x['time_index_key']) == sorted(expected, key=lambda x: x['time_index_key'])

    return comparator


@pytest.fixture
def ingester(storage, sqs_queue, sns_topic_arn):
    reporter = SNSReporter(sns_topic_arn)
    return Ingester(storage, queue=sqs_queue, reporter=reporter)


@mock.patch.dict(os.environ, {"AWS_REGION":"us-east-1"})
def test_listener_reports(event_test_driver, ingester, sqs_sender, records_comparator):

    def tester(event):
        sqs_sender(event['s3_notification'])
        ingester.listen(timeout=1)
        records_comparator(event['expected_datalake_records'])

    expected_reports = event_test_driver(tester)

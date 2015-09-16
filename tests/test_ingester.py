import pytest
from datalake_common.tests import random_metadata
import time
import os

from datalake_ingester import DynamoDBStorage, Ingester, \
    InvalidS3Error, SQSQueue, SNSReporter
from datalake_common.errors import InsufficientConfiguration

from conftest import all_s3_notification_specs, all_bad_s3_notification_specs


@pytest.fixture
def storage(dynamodb_records_table, dynamodb_connection):
    return DynamoDBStorage(table_name='records',
                           connection=dynamodb_connection)


@pytest.fixture
def random_s3_file_maker(s3_file_from_metadata, random_metadata):
    def maker():
        url = 's3://foo/' + random_metadata['id']
        s3_file_from_metadata(url, random_metadata)
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
    return Ingester(storage, queue=sqs_queue)


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


@pytest.fixture
def report_listener(bare_sqs_queue_maker, sns_connection, sns_topic_arn):

    class ReportListener(object):

        def __init__(self):
            self.messages = []
            q = bare_sqs_queue_maker('reporter-queue')
            self._queue = SQSQueue(q.name, self.handler)
            sns_connection.subscribe_sqs_queue(sns_topic_arn, q)

        def handler(self, msg):
            self.messages.append(msg)

        def drain(self):
            self._queue.drain(timeout=1)

    return ReportListener()


@pytest.fixture
def full_ingester(storage, sqs_queue, sns_topic_arn):
    reporter = SNSReporter(sns_topic_arn)
    return Ingester(storage, queue=sqs_queue, reporter=reporter)


@pytest.fixture
def report_comparator():

    def sort(l):
        return sorted(l, key=lambda k: k['url'])

    def comparator(actual, expected):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            err = abs(time.time() - a['start']/1000.0)
            assert err < 5.0
            assert sort(a['records']) == sort(e['records'])
            assert type(a['duration']) is float
            assert a['status'] == e['status']
            if e['status'] == 'error':
                assert 'message' in a
    return comparator


@pytest.fixture(params=all_s3_notification_specs)
def listener_report_tester(request, full_ingester, report_listener, sqs_sender,
                           spec_maker, report_comparator):
    spec = spec_maker(request.param)
    sqs_sender(spec['s3_notification'])
    full_ingester.listen(timeout=1)
    report_listener.drain()
    report_comparator(report_listener.messages, spec['expected_reports'])


def test_listener_reports(listener_report_tester):
    pass


@pytest.fixture(params=all_bad_s3_notification_specs)
def bad_notification_ingester(request, ingester_with_queue, sqs_sender,
                              spec_maker):
    def ingester():
        spec = spec_maker(request.param)
        sqs_sender(spec['s3_notification'])
        return ingester_with_queue
    return ingester

def test_bad_reports_raise(bad_notification_ingester):
    with pytest.raises(InvalidS3Error):
        bad_notification_ingester().listen(timeout=1)


@pytest.fixture
def full_catchy_ingester(storage, sqs_queue, sns_topic_arn):
    reporter = SNSReporter(sns_topic_arn)
    return Ingester(storage, queue=sqs_queue, reporter=reporter,
                    catch_exceptions=True)


@pytest.fixture(params=all_bad_s3_notification_specs)
def bad_ingestion_reporter(request, full_catchy_ingester, report_listener,
                           sqs_sender, spec_maker):
    def reporter():
        spec = spec_maker(request.param)
        sqs_sender(spec['s3_notification'])
        full_catchy_ingester.listen(timeout=1)
        report_listener.drain()
        return report_listener.messages, spec['expected_reports']

    return reporter


def test_bad_ingestion_reports(bad_ingestion_reporter, report_comparator):
    actual, expected = bad_ingestion_reporter()
    report_comparator(actual, expected)

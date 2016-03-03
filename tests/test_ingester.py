import pytest
import time


from datalake_ingester import DynamoDBStorage, Ingester, \
    SQSQueue, SNSReporter
from datalake_common.errors import InsufficientConfiguration


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
def records_comparator(dynamodb_records_table):

    def comparator(expected_records):
        records = [dict(r) for r in dynamodb_records_table.scan()]
        assert sorted(records) == sorted(expected_records)

    return comparator


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
def ingester(storage, sqs_queue, sns_topic_arn):
    reporter = SNSReporter(sns_topic_arn)
    return Ingester(storage, queue=sqs_queue, reporter=reporter)


@pytest.fixture
def report_comparator():

    def sort(l):
        return sorted(l, key=lambda k: k['url'])

    def comparator(actual, expected):
        if expected is None:
            return
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


def test_listener_reports(event_test_driver, ingester, report_listener,
                          sqs_sender, report_comparator):

    def tester(event):
        sqs_sender(event['s3_notification'])
        ingester.listen(timeout=1)
        records_comparator(event['expected_datalake_records'])

    expected_reports = event_test_driver(tester)
    report_listener.drain()
    report_comparator(report_listener.messages, expected_reports)

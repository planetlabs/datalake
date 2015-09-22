import pytest
from datalake_common import DatalakeRecord
from datalake_common.tests import random_metadata

from datalake_api.querier import ArchiveQuerier


@pytest.fixture
def archive_querier(dynamodb):
    return ArchiveQuerier('test', dynamodb=dynamodb)


def create_test_records(**kwargs):
    m = random_metadata()
    m.update(**kwargs)
    url = '/'.join(['s3'] + [str(v) for v in kwargs.values()])
    return DatalakeRecord.list_from_metadata(url, m)


def in_url(result, part):
    url = result['url']
    parts = url.split('/')
    return part in parts


def in_metadata(result, **kwargs):
    m = result['metadata']
    return all([k in m and m[k] == kwargs[k] for k in kwargs.keys()])


def all_results(results, **kwargs):
    assert len(results) >= 1
    return all([in_metadata(r, **kwargs) for r in results])


def result_between(result, start, end):
    assert start < end
    assert result['metadata']['start'] < result['metadata']['end']
    if result['metadata']['end'] < start:
        return False
    if result['metadata']['start'] > end:
        return False
    return True


def all_results_between(results, start, end):
    assert len(results) >= 1
    return all([result_between(r, start, end) for r in results])


def test_query_by_work_id(table_maker, archive_querier):
    records = []
    for i in range(2):
        work_id = 'work{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo')
    table = table_maker(records)
    results = archive_querier.query_by_work_id('work0', 'foo')
    assert len(results) == 1
    assert all_results(results, work_id='work0')


def test_query_work_id_with_where(table_maker, archive_querier):
    records = []
    for i in range(4):
        work_id = 'work0'
        where = 'worker{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo', where=where)
    table = table_maker(records)
    results = archive_querier.query_by_work_id('work0', 'foo', where='worker0')
    assert len(results) == 1
    assert all_results(results, work_id='work0', where='worker0')


def test_query_by_time(table_maker, archive_querier):
    records = []
    for start in range(0, 100, 10):
        end = start + 9
        records += create_test_records(start=start, end=end, what='foo')
    table = table_maker(records)
    results = archive_querier.query_by_time(0, 9, 'foo')
    assert len(results) == 1
    assert all_results_between(results, 0, 9)


def test_query_by_time_with_where(table_maker, archive_querier):
    records = []
    for i in range(4):
        where = 'worker{}'.format(i)
        records += create_test_records(start=0, end=10, what='foo', where=where)

    table = table_maker(records)
    results = archive_querier.query_by_time(0, 10, 'foo', where='worker2')
    assert len(results) == 1
    assert all_results(results, start=0, end=10, where='worker2')
    assert all_results_between(results, 0, 10)


def test_deduplicating_time_records(table_maker, archive_querier):
    # Create a record that definitively spans two time buckets, and make sure
    # that we only get one record back when we query for it.
    start = 0
    end = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = create_test_records(start=start, end=end, what='foo')
    table = table_maker(records)
    results = archive_querier.query_by_time(start, 2*end, 'foo')
    assert len(results) == 1


def test_deduplicating_work_id_records(table_maker, archive_querier):
    start = 0
    end = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = create_test_records(start=start, end=end, what='foo',
                                  work_id='job0')
    table = table_maker(records)
    results = archive_querier.query_by_work_id('job0', 'foo')
    assert len(results) == 1

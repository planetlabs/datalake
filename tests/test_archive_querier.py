import pytest
from datalake_common import DatalakeRecord
from datalake_common.tests import random_metadata
import simplejson as json

from datalake_api.querier import ArchiveQuerier, MAX_RESULTS
from conftest import client

# we run all of the tests in this file against both the ArchiveQuerier and the
# HTTP API. To achieve the latter, we wrap up the flask test client in an
# object that looks like an ArchiveQuerier and returns HttpResults.

class HttpResults(list):

    def __init__(self, result):
        assert result.status_code == 200
        response = json.loads(result.get_data())
        for k in ['next', 'metadata']:
            assert k in response
        super(HttpResults, self).__init__(response['metadata'])


class HttpQuerier(object):

    def __init__(self, *args, **kwargs):
        self.client = client()

    def query_by_work_id(self, work_id, what, where=None, cursor=None):
        params = dict(
            work_id=work_id,
            what=what,
            where=where,
            cursor=cursor,
        )
        result = self._do_query(params)
        return HttpResults(result)

    def query_by_time(self, start, end, what, where=None, cursor=None):
        params = dict(
            start=start,
            end=end,
            what=what,
            where=where,
            cursor=cursor,
        )
        result = self._do_query(params)
        return HttpResults(result)

    def _do_query(self, params):
        uri = '/v0/archive/files/'
        params = ['{}={}'.format(k, v) for k, v in params.iteritems()
                  if v is not None]
        q = '&'.join(params)
        if q:
            uri += '?' + q
        return self.client.get(uri)


@pytest.fixture(params=[ArchiveQuerier, HttpQuerier],
                ids=['archive_querier', 'http'])
def querier(request, dynamodb):
    return request.param('test', dynamodb=dynamodb)


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
    return all([k in result and result[k] == kwargs[k] for k in kwargs.keys()])


def all_results(results, **kwargs):
    assert len(results) >= 1
    return all([in_metadata(r, **kwargs) for r in results])


def result_between(result, start, end):
    assert start < end
    assert result['start'] < result['end']
    if result['end'] < start:
        return False
    if result['start'] > end:
        return False
    return True


def all_results_between(results, start, end):
    assert len(results) >= 1
    return all([result_between(r, start, end) for r in results])


def test_query_by_work_id(table_maker, querier):
    records = []
    for i in range(2):
        work_id = 'work{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo')
    table = table_maker(records)
    results = querier.query_by_work_id('work0', 'foo')
    assert len(results) == 1
    assert all_results(results, work_id='work0')


def test_query_work_id_with_where(table_maker, querier):
    records = []
    for i in range(4):
        work_id = 'work0'
        where = 'worker{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo', where=where)
    table = table_maker(records)
    results = querier.query_by_work_id('work0', 'foo', where='worker0')
    assert len(results) == 1
    assert all_results(results, work_id='work0', where='worker0')


def test_query_by_time(table_maker, querier):
    records = []
    for start in range(0, 100, 10):
        end = start + 9
        records += create_test_records(start=start, end=end, what='foo')
    table = table_maker(records)
    results = querier.query_by_time(0, 9, 'foo')
    assert len(results) == 1
    assert all_results_between(results, 0, 9)


def test_query_by_time_with_where(table_maker, querier):
    records = []
    for i in range(4):
        where = 'worker{}'.format(i)
        records += create_test_records(start=0, end=10, what='foo', where=where)

    table = table_maker(records)
    results = querier.query_by_time(0, 10, 'foo', where='worker2')
    assert len(results) == 1
    assert all_results(results, start=0, end=10, where='worker2')
    assert all_results_between(results, 0, 10)


def test_deduplicating_time_records(table_maker, querier):
    # Create a record that definitively spans two time buckets, and make sure
    # that we only get one record back when we query for it.
    start = 0
    end = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = create_test_records(start=start, end=end, what='foo')
    table = table_maker(records)
    results = querier.query_by_time(start, 2*end, 'foo')
    assert len(results) == 1


def test_deduplicating_work_id_records(table_maker, querier):
    start = 0
    end = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = create_test_records(start=start, end=end, what='foo',
                                  work_id='job0')
    table = table_maker(records)
    results = querier.query_by_work_id('job0', 'foo')
    assert len(results) == 1


def test_paginate_work_id_records(table_maker, querier):
    records = []
    for i in range(150):
        records += create_test_records(what='foo', work_id='job0')
    table = table_maker(records)

    results = []
    cursor = None
    while True:
        page = querier.query_by_work_id('job0', 'foo', cursor=cursor)
        page_len = len(page)
        assert page_len <= MAX_RESULTS
        if len(results) == 0:
            assert page.cursor is not None
        results += page
        cursor = page.cursor
        if cursor is None:
            break
    assert len(results) == 150


def test_paginate_time_records(table_maker, querier):
    records = []
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    very_end = 150 * interval
    for start in range(0, very_end, interval):
        end = start + interval
        records += create_test_records(start=start, end=end, what='foo')
    table = table_maker(records)

    results = []
    cursor = None
    while True:
        page = querier.query_by_time(0, very_end, 'foo', cursor=cursor)
        page_len = len(page)
        assert page_len <= MAX_RESULTS
        if len(results) == 0:
            assert page.cursor is not None
        results += page
        cursor = page.cursor
        if cursor is None:
            break
    # we tolerate some duplication for time queries because there is no great
    # way to deduplicate across pages.
    assert len(results) >= 150
    ids = set([r['id'] for r in results])
    assert len(ids) == 150

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
from datalake_common import DatalakeRecord
from datalake_common.tests import random_metadata
import simplejson as json
from urlparse import urlparse
import time
from datalake_api.querier import ArchiveQuerier, MAX_RESULTS
from conftest import client
from conftest import create_test_records


_ONE_DAY_MS = 24 * 60 * 60 * 1000


# we run all of the tests in this file against both the ArchiveQuerier and the
# HTTP API. To achieve the latter, we wrap up the flask test client in an
# object that looks like an ArchiveQuerier and returns HttpResults.


class HttpResults(list):

    def __init__(self, result):
        assert result.status_code == 200
        self.response = json.loads(result.get_data())
        self._validate_response()
        records = [HttpRecord(**r) for r in self.response['records']]
        super(HttpResults, self).__init__(records)

    def _validate_response(self):
        for k in ['next', 'records']:
            assert k in self.response
        self._validate_next_url(self.response['next'])

    def _validate_next_url(self, next):
        if next is None:
            return
        parts = urlparse(next)
        assert 'cursor=' in parts.query

    @property
    def cursor(self):
        return self.response['next']


class HttpRecord(dict):

    def __init__(self, **kwargs):
        super(HttpRecord, self).__init__(**kwargs)
        self._validate()

    def _validate(self):
        assert 'http_url' in self
        assert self['http_url'].startswith('http')
        assert self['http_url'].endswith(self['metadata']['id'] + '/data')


class HttpQuerier(object):

    def __init__(self, *args, **kwargs):
        self.client = client()

    def query_by_work_id(self, work_id, what, where=None, cursor=None):
        params = dict(
            work_id=work_id,
            what=what,
            where=where,
        )
        return self._query_or_next(params, cursor)

    def query_by_time(self, start, end, what, where=None, cursor=None):
        params = dict(
            start=start,
            end=end,
            what=what,
            where=where,
        )
        return self._query_or_next(params, cursor)

    def _query_or_next(self, params, cursor):
        if cursor is None:
            result = self._do_query(params)
        else:
            result = self._get_next(cursor)
        return HttpResults(result)

    def _do_query(self, params):
        uri = '/v0/archive/files/'
        params = ['{}={}'.format(k, v) for k, v in params.iteritems()
                  if v is not None]
        q = '&'.join(params)
        if q:
            uri += '?' + q
        return self.client.get(uri)

    def _get_next(self, cursor):
        # the "cursor" is the next URL in this case

        # Work around this issue with the flask test client:
        # https://github.com/mitsuhiko/flask/issues/968
        cursor = '/'.join([''] + cursor.split('/')[3:])
        return self.client.get(cursor)

    def query_latest(self, what, where):
        uri = '/v0/archive/latest/{}/{}'.format(what, where)
        result = self.client.get(uri)
        if result.status_code == 404:
            return None
        assert result.status_code == 200
        record = json.loads(result.get_data())
        return HttpRecord(**record)


@pytest.fixture(params=[ArchiveQuerier, HttpQuerier],
                ids=['archive_querier', 'http'])
def querier(request, dynamodb):
    return request.param('test', dynamodb=dynamodb)


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


def test_query_by_work_id(table_maker, querier):
    records = []
    for i in range(2):
        work_id = 'work{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo')
    table_maker(records)
    results = querier.query_by_work_id('work0', 'foo')
    assert len(results) == 1
    assert all_results(results, work_id='work0')


def test_query_work_id_with_where(table_maker, querier):
    records = []
    for i in range(4):
        work_id = 'work0'
        where = 'worker{}'.format(i)
        records += create_test_records(work_id=work_id, what='foo',
                                       where=where)
    table_maker(records)
    results = querier.query_by_work_id('work0', 'foo', where='worker0')
    assert len(results) == 1
    assert all_results(results, work_id='work0', where='worker0')


def test_query_by_time(table_maker, querier):
    records = []
    for start in range(0, 100, 10):
        end = start + 9
        records += create_test_records(start=start, end=end, what='foo')
    table_maker(records)
    results = querier.query_by_time(0, 9, 'foo')
    assert len(results) == 1
    assert all_results_between(results, 0, 9)


def test_query_by_time_with_where(table_maker, querier):
    records = []
    for i in range(4):
        where = 'worker{}'.format(i)
        records += create_test_records(start=0, end=10, what='foo',
                                       where=where)

    table_maker(records)
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
    table_maker(records)
    results = querier.query_by_time(start, 2*end, 'foo')
    assert len(results) == 1


def test_deduplicating_work_id_records(table_maker, querier):
    start = 0
    end = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = create_test_records(start=start, end=end, what='foo',
                                  work_id='job0')
    table_maker(records)
    results = querier.query_by_work_id('job0', 'foo')
    assert len(results) == 1


def get_multiple_pages(query_function, query_args):
    results = []
    cursor = None
    while True:
        page = query_function(*query_args, cursor=cursor)
        page_len = len(page)
        assert page_len <= MAX_RESULTS
        # Don't allow empty pages
        assert page_len > 0
        results += page
        cursor = page.cursor
        if cursor is None:
            break
    return results


def test_paginate_work_id_records(table_maker, querier):
    records = []
    for i in range(150):
        records += create_test_records(what='foo', work_id='job0',
                                       start=1456833600000,
                                       end=1456837200000)
    table_maker(records)
    results = get_multiple_pages(querier.query_by_work_id, ['job0', 'foo'])
    assert len(results) == 150


def evaluate_time_based_results(results, num_expected):
    # we tolerate some duplication for time queries because there is no great
    # way to deduplicate across pages.
    assert len(results) >= num_expected
    ids = set([r['metadata']['id'] for r in results])
    assert len(ids) == num_expected


def test_paginate_time_records(table_maker, querier):
    records = []
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    very_end = 150 * interval
    for start in range(0, very_end, interval):
        end = start + interval
        records += create_test_records(start=start, end=end, what='foo')
    table_maker(records)
    results = get_multiple_pages(querier.query_by_time, [0, very_end, 'foo'])
    evaluate_time_based_results(results, 150)


def test_paginate_many_records_single_time_bucket(table_maker, querier):
    records = []
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS/150
    very_end = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    for start in range(0, very_end, interval):
        end = start + interval
        records += create_test_records(start=start, end=end, what='foo')
    table_maker(records)
    results = get_multiple_pages(querier.query_by_time, [0, very_end, 'foo'])
    evaluate_time_based_results(results, 150)


def test_paginate_few_records_single_bucket_no_empty_page(table_maker, querier):
    records = []
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS * 2 / MAX_RESULTS
    very_end = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    for start in range(0, very_end, interval):
        end = start + interval
        records += create_test_records(start=start, end=end, what='foo')
    table_maker(records)
    results = get_multiple_pages(querier.query_by_time, [very_end - 2 * interval + 1, very_end, 'foo'])
    evaluate_time_based_results(results, 2)


def test_null_end(table_maker, querier):
    m = {
        "start": 1461023640000,
        "what": "file",
        "version": 0,
        "end": None,
        "work_id": None,
        "path": "/home/foo/file",
        "where": "somehost",
        "id": "fedcba09876543210",
        "hash": "0123456789abcdef"
    }
    url = 's3://datalake-test/' + m['id']
    records = DatalakeRecord.list_from_metadata(url, m)
    table_maker(records)
    results = querier.query_by_time(1461023630000, 1461023650000, 'file')
    assert len(results) == 1


def test_no_end(table_maker, querier):
    m = random_metadata()
    del(m['end'])
    url = 's3://datalake-test/' + m['id']
    records = DatalakeRecord.list_from_metadata(url, m)
    table_maker(records)
    results = querier.query_by_time(m['start'], m['start'] + 1, m['what'])
    assert len(results) == 1


def test_no_end_exclusion(table_maker, querier):
    m = random_metadata()
    del(m['end'])
    url = 's3://datalake-test/' + m['id']
    records = DatalakeRecord.list_from_metadata(url, m)
    table_maker(records)
    results = querier.query_by_time(m['start'] + 1, m['start'] + 2, m['what'])
    assert len(results) == 0


def _validate_latest_result(result, **kwargs):
    assert result is not None
    for k, v in kwargs.iteritems():
        assert result['metadata'][k] == v


def test_latest_happened_today(table_maker, querier):
    now = int(time.time() * 1000)
    records = create_test_records(start=now, end=None, what='foo', where='boo')
    table_maker(records)
    result = querier.query_latest('foo', 'boo')
    _validate_latest_result(result, what='foo', where='boo')


def test_no_latest(table_maker, querier):
    table_maker([])
    result = querier.query_latest('statue', 'newyork')
    assert result is None


def test_latest_happened_yesterday(table_maker, querier):
    yesterday = int(time.time() * 1000) - _ONE_DAY_MS
    records = create_test_records(start=yesterday, end=None, what='tower',
                                  where='pisa')
    table_maker(records)
    result = querier.query_latest('tower', 'pisa')
    _validate_latest_result(result, what='tower', where='pisa')


def test_latest_many_records_single_time_bucket(table_maker, querier):
    now = int(time.time() * 1000)
    records = []
    bucket = now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS/150
    very_end = start + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    last_start = very_end - interval
    for t in range(start, very_end, interval):
        end = t + interval
        records += create_test_records(start=t, end=end,
                                       what='meow', where='tree')
    table_maker(records)
    result = querier.query_latest('meow', 'tree')
    _validate_latest_result(result, what='meow', where='tree',
                            start=last_start)

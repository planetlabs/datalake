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
from datalake.common import DatalakeRecord
from datalake.tests import generate_random_metadata
import simplejson as json
from urllib.parse import urlparse
import time
from datalake_api.querier import ArchiveQuerier, MAX_RESULTS
from conftest import get_client, YEAR_2010


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
        self.client = get_client()

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
        params = ['{}={}'.format(k, v) for k, v in params.items()
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
        return request.param('test', 'test_latest', dynamodb=dynamodb)

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


def test_query_by_work_id(table_maker, querier, record_maker):
    records = []
    for i in range(2):
        work_id = 'work{}'.format(i)
        records += record_maker(work_id=work_id, what='foo')
    table_maker(records)
    results = querier.query_by_work_id('work0', 'foo')
    assert len(results) == 1
    assert all_results(results, work_id='work0')


def test_query_work_id_with_where(table_maker, querier, record_maker):
    records = []
    for i in range(4):
        work_id = 'work0'
        where = 'worker{}'.format(i)
        records += record_maker(work_id=work_id, what='foo', where=where)
    table_maker(records)
    results = querier.query_by_work_id('work0', 'foo', where='worker0')
    assert len(results) == 1
    assert all_results(results, work_id='work0', where='worker0')


def test_query_by_time(table_maker, querier, record_maker):
    records = []
    for start in range(YEAR_2010, YEAR_2010+100, 10):
        end = start + 9
        records += record_maker(start=start, end=end, what='foo')
    table_maker(records)
    results = querier.query_by_time(YEAR_2010, YEAR_2010+9, 'foo')
    assert len(results) == 1
    assert all_results_between(results, YEAR_2010, YEAR_2010+9)


def test_query_by_time_with_where(table_maker, querier, record_maker):
    records = []
    for i in range(4):
        where = 'worker{}'.format(i)
        records += record_maker(start=YEAR_2010, end=YEAR_2010+10,
                                what='foo', where=where)

    table_maker(records)
    results = querier.query_by_time(YEAR_2010, YEAR_2010+10, 'foo',
                                    where='worker2')
    assert len(results) == 1
    assert all_results(results, start=YEAR_2010, end=YEAR_2010+10,
                       where='worker2')
    assert all_results_between(results, YEAR_2010, YEAR_2010+10)


def test_deduplicating_time_records(table_maker, querier, record_maker):
    # Create a record that definitively spans two time buckets, and make sure
    # that we only get one record back when we query for it.
    start = YEAR_2010
    two_buckets = 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    end = YEAR_2010 + two_buckets
    records = record_maker(start=start, end=end, what='foo')
    table_maker(records)
    results = querier.query_by_time(start, end+two_buckets, 'foo')
    assert len(results) == 1


def test_deduplicating_work_id_records(table_maker, querier, record_maker):
    start = YEAR_2010
    end = YEAR_2010 + 2 * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    records = record_maker(start=start, end=end, what='foo', work_id='job0')
    table_maker(records)
    results = querier.query_by_work_id('job0', 'foo')
    assert len(results) == 1


def get_multiple_pages(query_function, query_args):
    results = []
    cursor = None
    while True:
        page = get_page(query_function, query_args, cursor)
        # Ensure the first page has a cursor
        if len(results) == 0:
            assert page.cursor is not None
        results += page
        cursor = page.cursor
        if cursor is None:
            break
    return results


def get_all_pages(query_function, query_args):
    pages = []
    cursor = None
    while True:
        page = get_page(query_function, query_args, cursor)
        pages.append(page)
        cursor = page.cursor
        if cursor is None:
            break
    return pages


def get_page(query_function, query_args, cursor=None):
    page = query_function(*query_args, cursor=cursor)
    page_len = len(page)
    assert page_len <= MAX_RESULTS
    # Only allow the last page to be empty
    if page.cursor is not None:
        assert page_len > 0
    return page


def consolidate_pages(pages):
    for p in pages[:-1]:
        assert p.cursor is not None
    return [record for page in pages for record in page]


# TODO: The version of moto that we are using is broken for certain dynamodb
# FilterExpressions (see https://github.com/spulec/moto/issues/3909). In our
# case, this causes an infinite loop in queries that require pagination. This
# issue is fixed in recent releases of moto. But we can't upgrade to those
# until we migrate from boto to boto3. So for now, we skip those tests.

import moto  # noqa
moto_major = int(moto.__version__.split('.')[0])


@pytest.mark.skipif(moto_major < 3, reason='moto: issue 3909')
def test_paginate_work_id_records(table_maker, querier, record_maker):
    # TODO!! Let's make this test pass with the deduplication of records. Not
    # sure why it doesn't work.
    records = []
    for i in range(150):
        records += record_maker(what='foo', work_id='job0',
                                start=1456833600000,
                                end=1456837200000)
    table_maker(records)
    pages = get_all_pages(querier.query_by_work_id, ['job0', 'foo'])
    assert len(pages) > 1
    results = consolidate_pages(pages)
    assert len(results) == 150


def evaluate_time_based_results(results, num_expected):
    # we tolerate some duplication for time queries because there is no great
    # way to deduplicate across pages.
    assert len(results) >= num_expected
    ids = set([r['metadata']['id'] for r in results])
    assert len(ids) == num_expected


def test_paginate_time_records(table_maker, querier, record_maker):
    records = []
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    very_end = YEAR_2010 + 150 * interval
    for start in range(YEAR_2010, very_end, interval):
        end = start + interval
        records += record_maker(start=start, end=end, what='foo')
    table_maker(records)
    pages = get_all_pages(querier.query_by_time, [YEAR_2010, very_end, 'foo'])
    assert len(pages) > 1
    results = consolidate_pages(pages)
    evaluate_time_based_results(results, 150)


@pytest.mark.skipif(moto_major < 3, reason='moto: issue 3909')
def test_paginate_many_records_single_time_bucket(table_maker, querier,
                                                  record_maker):
    records = []
    interval = int(DatalakeRecord.TIME_BUCKET_SIZE_IN_MS/150)
    very_end = YEAR_2010 + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    for start in range(YEAR_2010, very_end, interval):
        end = start + interval
        records += record_maker(start=start, end=end, what='foo')
    table_maker(records)
    pages = get_all_pages(querier.query_by_time, [YEAR_2010, very_end, 'foo'])
    assert len(pages) > 1
    results = consolidate_pages(pages)
    evaluate_time_based_results(results, 150)


@pytest.mark.skipif(moto_major < 3, reason='moto: issue 3909')
def test_paginate_few_records_single_bucket_no_empty_page(table_maker,
                                                          querier,
                                                          record_maker):
    records = []
    # Fill one bucket with 2x MAX_RESULTS,
    # but we only want the last record.
    interval = int(DatalakeRecord.TIME_BUCKET_SIZE_IN_MS / MAX_RESULTS / 2)
    very_end = YEAR_2010 + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    for start in range(YEAR_2010, very_end, interval):
        end = start + interval
        records += record_maker(start=start, end=end, what='foo')
    table_maker(records)
    results = get_page(querier.query_by_time, [very_end - interval + 1,
                       very_end, 'foo'])
    evaluate_time_based_results(results, 1)


def test_unaligned_multibucket_queries(table_maker, querier, record_maker):
    records = []

    # Create 5 records spanning 3 buckets, of which we want the middle 3
    records += record_maker(
        start=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*1/4,
        end=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*1/4+1, what='foo')
    records += record_maker(
        start=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*3/4,
        end=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*3/4+1, what='foo')
    records += record_maker(
        start=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*6/4,
        end=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*6/4+1, what='foo')
    records += record_maker(
        start=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*9/4,
        end=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*9/4+1, what='foo')
    records += record_maker(
        start=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*11/4,
        end=YEAR_2010+DatalakeRecord.TIME_BUCKET_SIZE_IN_MS*11/4+1, what='foo')

    table_maker(records)
    start = YEAR_2010 + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS * 3 / 4
    end = YEAR_2010 + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS * 9 / 4
    results = get_page(querier.query_by_time, [start, end, 'foo'])
    evaluate_time_based_results(results, 3)


def test_null_end(table_maker, querier, record_maker):
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
    records = record_maker(**m)
    table_maker(records)
    results = querier.query_by_time(1461023630000, 1461023650000, 'file')
    assert len(results) == 1


def test_no_end(table_maker, querier, s3_file_from_metadata):
    m = generate_random_metadata()
    del(m['end'])
    url = 's3://datalake-test/' + m['id']
    s3_file_from_metadata(url, m)
    records = DatalakeRecord.list_from_metadata(url, m)
    for record in records:
        what = record.get('what')
        where = record.get('where')
        record['what_where_key'] = f'{what}:{where}'
    table_maker(records)
    results = querier.query_by_time(m['start'], m['start'] + 1, m['what'])
    assert len(results) == 1
    assert results[0]['metadata']['end'] is None


def test_no_end_exclusion(table_maker, querier, s3_file_from_metadata):
    m = generate_random_metadata()
    del(m['end'])
    url = 's3://datalake-test/' + m['id']
    s3_file_from_metadata(url, m)
    records = DatalakeRecord.list_from_metadata(url, m)
    for record in records:
        what = record.get('what')
        where = record.get('where')
        record['what_where_key'] = f'{what}:{where}'
    table_maker(records)
    
    results = querier.query_by_time(m['start'] + 1, m['start'] + 2, m['what'])
    assert len(results) == 0


def _validate_latest_result(result, **kwargs):
    assert result is not None
    for k, v in kwargs.items():
        assert result['metadata'][k] == v


def test_latest_happened_today(table_maker, querier, record_maker):
    now = int(time.time() * 1000)
    records = record_maker(start=now, end=None, what='foo', where='boo')
    table_maker(records)
    result = querier.query_latest('foo', 'boo')
    _validate_latest_result(result, what='foo', where='boo')


def test_no_latest(table_maker, querier):
    table_maker([])
    result = querier.query_latest('statue', 'newyork')
    assert result is None


def test_latest_happened_yesterday(table_maker, querier, record_maker):
    yesterday = int(time.time() * 1000) - _ONE_DAY_MS
    records = record_maker(start=yesterday, end=None, what='tower',
                           where='pisa')
    table_maker(records)
    result = querier.query_latest('tower', 'pisa')
    _validate_latest_result(result, what='tower', where='pisa')


def test_latest_many_records_single_time_bucket(table_maker, querier,
                                                record_maker):
    now = int(time.time() * 1000)
    records = []
    bucket = int(now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS)
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    interval = int(DatalakeRecord.TIME_BUCKET_SIZE_IN_MS/150)
    very_end = start + DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    last_start = very_end - interval
    for t in range(start, very_end, interval):
        end = t + interval
        records += record_maker(start=t, end=end, what='meow', where='tree')
    table_maker(records)
    result = querier.query_latest('meow', 'tree')
    _validate_latest_result(result, what='meow', where='tree',
                            start=last_start)


def test_latest_creation_time_breaks_tie(table_maker, querier,
                                         record_maker):
    now = int(time.time() * 1000)
    bucket = int(now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS)
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    interval = DatalakeRecord.TIME_BUCKET_SIZE_IN_MS/150
    end = start + interval
    table = table_maker([])[0]
    for i in range(3):
        record = record_maker(start=start,
                              end=end,
                              what='meow',
                              where='tree',
                              path='/{}'.format(i))
        table.put_item(Item=record[0])
        # unfortunately moto only keeps 1-sec resolution on create times.
        time.sleep(1.01)
    result = querier.query_latest('meow', 'tree')
    _validate_latest_result(result, what='meow', where='tree',
                            start=start)
    assert result['metadata']['path'] == '/2'


def test_max_results_in_one_bucket(table_maker, querier, record_maker):
    now = int(time.time() * 1000)
    records = []
    bucket = int(now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS)
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    end = start
    for i in range(MAX_RESULTS):
        records += record_maker(start=start,
                                end=end,
                                what='boo',
                                where='hoo{}'.format(i))
    print(f'records are {records}')
    table_maker(records)
    pages = get_all_pages(querier.query_by_time, [start, end, 'boo'])
    results = consolidate_pages(pages)
    assert len(results) == MAX_RESULTS


@pytest.mark.skipif(moto_major < 3, reason='moto: issue 3909')
def test_2x_max_results_in_one_bucket(table_maker, querier, record_maker):
    now = int(time.time() * 1000)
    records = []
    bucket = int(now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS)
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    end = start
    for i in range(MAX_RESULTS * 2):
        records += record_maker(start=start,
                                end=end,
                                what='boo',
                                where='hoo{}'.format(i))
    table_maker(records)
    pages = get_all_pages(querier.query_by_time, [start, end, 'boo'])
    results = consolidate_pages(pages)
    assert len(results) == MAX_RESULTS * 2


def test_latest_table_query(table_maker, querier, record_maker):
    now = int(time.time() * 1000)
    records = []
    bucket = int(now/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS)
    start = bucket * DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
    end = start
    for i in range(MAX_RESULTS):
        records += record_maker(start=start,
                                end=end,
                                what='boo',
                                where='hoo{}'.format(i))
    table_maker(records)
    querier.use_latest = True
    result = querier.query_latest('boo', 'hoo0')
    _validate_latest_result(result, what='boo', where='hoo0')
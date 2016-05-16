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

from memoized_property import memoized_property
from boto3.dynamodb.conditions import Key, And, Not, Attr
from datalake_common import DatalakeRecord
import base64
import simplejson as json
import time


'''the maximum number of results to return to the user

dynamodb will return a max of 1MB to us. And our documents could be
~2kB. Keeping MAX_RESULTS at 100 keeps us from hitting this limit.
'''
MAX_RESULTS = 100


'''the maximum number of days to lookback for latest files

We do not index the latest files as such. Instead, we naively scan backwards
through each time bucket looking for the expected file. We will not look
arbitrarily far back, though, because this makes the failing case terribly
slow.
'''
MAX_LOOKBACK_DAYS = 14


_ONE_DAY_MS = 24 * 60 * 60 * 1000


class InvalidCursor(Exception):
    pass


class Cursor(dict):
    '''a cursor to retrieve the next page of results in a query

    We never return more than MAX_RESULTS to the user. For work_id-based
    queries, we achive this by passing Limit=MAX_RESULTS to dynamodb. If we get
    back a non-null LastEvaluated, we stash that in the cursor so we can pass
    it as ExclusiveStartKey. The LastEvaluated key contains the range key which
    contains the last ID that we saw. We use this to prevent sending duplicate
    records from page to page. This scheme is not perfect. For example, if
    there are many files with the same work-id that span many time buckets we
    will fail to deduplicate them. But this is a rare case.

    Time-based queries are a bit more complicated because we make one query to
    dynamodb for each time bucket. We query each bucket with
    Limit=MAX_RESULTS/2 until we have more than MAX_RESULTS/2 total results, or
    until we get a non-null LastEvaluated. We encode the current time bucket
    and LastEvaluated into the cursor. There's just no good way to guarantee
    that we deduplicate across pages. Minimally, we'd have to encode the
    last_id for every "where" in each batch into the cursor. This could get
    pretty unweildy. We still use the last ID that we saw to de-duplicate the
    (common?) case in which only a single "where" is in play.

    '''
    def __init__(self, **kwargs):
        '''create a new cursor

        Args:

        last_evaluated: The LastEvaluated value from a query with partial
        results.

        current_time_bucket: The time bucket being queried when the result
        limit was hit (not expected for work_id-based queries).

        last_id: The id of the last returned record. This is used to prevent
        duplication when the first record in the next page is the same as the
        last record in the previous page.

        '''
        super(Cursor, self).__init__(**kwargs)
        self._validate()

    def _validate(self):
        if 'last_evaluated' not in self and 'current_time_bucket' not in self:
            raise InvalidCursor('cursor missing required fields')

    @classmethod
    def from_serialized(cls, serialized):
        try:
            b64 = cls._apply_padding(serialized)
            j = base64.b64decode(b64)
            d = json.loads(j)
            return cls(**d)
        except json.JSONDecodeError:
            raise InvalidCursor('Failed to decode cursor ' + serialized)

    @staticmethod
    def _apply_padding(b64):
        padding_length = len(b64) % 4
        return b64 + '=' * padding_length

    @memoized_property
    def serialized(self):
        # the serialized representation of the cursor is a base64-encoded json
        # with the padding '=' stripped off the end. This makes it cleaner for
        # urls.
        b64 = base64.b64encode(self._json)
        return b64.rstrip('=')

    @memoized_property
    def _json(self):
        return json.dumps(self)

    @property
    def last_id(self):
        if 'last_id' in self:
            return self['last_id']
        elif self.last_evaluated:
            return self.last_evaluated['range_key'].split(':')[1]
        else:
            return None

    @property
    def last_evaluated(self):
        return self.get('last_evaluated')


class QueryResults(list):

    def __init__(self, results, cursor=None):
        results = self._deduplicate_and_unpack(results)
        super(QueryResults, self).__init__(results)
        self.cursor = cursor

    def _deduplicate_and_unpack(self, records):
        # http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
        seen = set()
        seen_add = seen.add
        unpack = self._unpack

        def _already_seen(r):
            id = r['metadata']['id']
            return id in seen or seen_add(id)

        return [unpack(r) for r in records if not _already_seen(r)]

    def _unpack(self, result):
        return dict(url=result['url'], metadata=result['metadata'])


class ArchiveQuerier(object):

    def __init__(self, table_name, dynamodb=None):
        self.table_name = table_name
        self.dynamodb = dynamodb

    def query_by_work_id(self, work_id, what, where=None, cursor=None):
        kwargs = self._prepare_work_id_kwargs(work_id, what, cursor)
        if where is not None:
            self._add_range_key_condition(kwargs, where)
        response = self._table.query(**kwargs)
        cursor = self._cursor_for_work_id_query(response)
        return QueryResults(response['Items'], cursor)

    def _prepare_work_id_kwargs(self, work_id, what, cursor):
        i = work_id + ':' + what
        kwargs = dict(
            IndexName='work-id-index',
            KeyConditionExpression=Key('work_id_index_key').eq(i),
            Limit=MAX_RESULTS,
        )
        if cursor is not None:
            self._add_cursor_conditions(kwargs, cursor)
        return kwargs

    def _add_range_key_condition(self, kwargs, where):
        condition = kwargs['KeyConditionExpression']
        where_condition = Key('range_key').begins_with(where + ':')
        new_condition = And(condition, where_condition)
        kwargs['KeyConditionExpression'] = new_condition

    def _cursor_for_work_id_query(self, response):
        last_evaluated = response.get('LastEvaluatedKey')
        if last_evaluated is None:
            return None
        return Cursor(last_evaluated=last_evaluated)

    def _add_cursor_conditions(self, kwargs, cursor):
        last_evaluated = cursor.get('last_evaluated')
        if last_evaluated is not None:
            kwargs['ExclusiveStartKey'] = last_evaluated
        if cursor.last_id is not None:
            known_duplicate = Attr('metadata.id').eq(cursor.last_id)
            kwargs['FilterExpression'] = Not(known_duplicate)

    def query_by_time(self, start, end, what, where=None, cursor=None):
        results = []
        buckets = DatalakeRecord.get_time_buckets(start, end)

        if cursor:
            current_bucket = cursor['current_time_bucket']
            i = buckets.index(current_bucket)
            buckets = buckets[i:]

        for b in buckets:
            kwargs = self._prepare_time_bucket_kwargs(b, what,
                                                      limit=MAX_RESULTS/2)
            if where is not None:
                self._add_range_key_condition(kwargs, where)
            if cursor is not None:
                self._add_cursor_conditions(kwargs, cursor)

            response = self._table.query(**kwargs)
            results += self._exclude_outside(response['Items'], start, end)
            # we _could_ deduplicate the results here to make more headroom for
            # another bucket.
            cursor = self._cursor_for_time_query(response, results, b)
            if cursor is not None:
                break

        return QueryResults(results, cursor)

    def _exclude_outside(self, records, start, end):
        return [r for r in records if self._intersects_time(r, start, end)]

    def _intersects_time(self, record, start, end):
        '''return true if a record intersects the specified time interval

        Note: the record may not have an 'end', or the 'end' may be None. In
        these cases, we only need to consider the 'start'.
        '''
        m = record['metadata']
        if 'end' not in m or m['end'] is None:
            if m['start'] < start or m['start'] > end:
                return False
            else:
                return True
        if m['end'] < start or m['start'] > end:
            return False
        return True

    def _prepare_time_bucket_kwargs(self, bucket, what, limit=None):
        i = str(bucket) + ':' + what
        kwargs = {
            'KeyConditionExpression': Key('time_index_key').eq(i)
        }
        if limit is not None:
            kwargs.update(Limit=limit)
        return kwargs

    def _cursor_for_time_query(self, response, results, current_bucket):
        last_evaluated = response.get('LastEvaluatedKey')

        if last_evaluated is None:
            if len(results) <= MAX_RESULTS/2:
                # there's enough headroom for another bucket.
                return None
            else:
                last_id = results[-1]['metadata']['id']
                return Cursor(current_time_bucket=current_bucket,
                              last_id=last_id)
        else:
            # Results from this time bucket did not fit in the page. Prepare
            # the cursor
            return Cursor(last_evaluated=last_evaluated,
                          current_time_bucket=current_bucket)

    @memoized_property
    def _table(self):
        return self.dynamodb.Table(self.table_name)

    def query_latest(self, what, where):
        current = int(time.time() * 1000)
        end = current - MAX_LOOKBACK_DAYS * _ONE_DAY_MS
        while current > end:
            bucket = current/DatalakeRecord.TIME_BUCKET_SIZE_IN_MS
            r = self._get_latest_record_in_bucket(bucket, what, where)
            if r is not None:
                return r
            current -= _ONE_DAY_MS

        return None

    def _get_latest_record_in_bucket(self, bucket, what, where):
        kwargs = self._prepare_time_bucket_kwargs(bucket, what)
        self._add_range_key_condition(kwargs, where)
        records = self._get_all_records_in_bucket(bucket, **kwargs)
        if not records:
            return None

        records = sorted(records, key=lambda r: r['metadata']['start'])
        result = records[-1]
        return dict(url=result['url'], metadata=result['metadata'])

    def _get_all_records_in_bucket(self, bucket, **kwargs):
        records = []
        while True:
            response = self._table.query(**kwargs)
            records += response['Items']
            if 'LastEvaluatedKey' not in response:
                break
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return records

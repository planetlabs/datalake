from memoized_property import memoized_property
from boto3.dynamodb.conditions import Key, And
from datalake_common import DatalakeRecord


class ArchiveQuerier(object):

    def __init__(self, table_name, dynamodb=None):
        self.table_name = table_name
        self.dynamodb = dynamodb

    def query_by_work_id(self, work_id, what, where=None):
        kwargs = self._prepare_work_id_kwargs(work_id, what)
        if where is not None:
            self._add_range_key_condition(kwargs, where)
        response = self._table.query(**kwargs)
        return self._deduplicate(response['Items'])

    def _prepare_work_id_kwargs(self, work_id, what):
        i = work_id + ':' + what
        return dict(
            IndexName='work-id-index',
            KeyConditionExpression=Key('work_id_index_key').eq(i)
        )

    def _add_range_key_condition(self, kwargs, where):
        condition = kwargs['KeyConditionExpression']
        new_condition = And(condition, Key('range_key').begins_with(where + ':'))
        kwargs['KeyConditionExpression'] = new_condition

    def _deduplicate(self, records):
        # http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
        seen = set()
        seen_add = seen.add

        def _already_seen(r):
            id = r['metadata']['id']
            return id in seen or seen_add(id)

        return [r for r in records if not _already_seen(r)]

    def query_by_time(self, start, end, what, where=None):
        results = []
        for b in DatalakeRecord.get_time_buckets(start, end):
            kwargs = self._prepare_time_bucket_kwargs(b, what)
            if where is not None:
                self._add_range_key_condition(kwargs, where)
            response = self._table.query(**kwargs)
            results += self._exclude_outside(response['Items'], start, end)
        return self._deduplicate(results)

    def _exclude_outside(self, records, start, end):
        return [r for r in records if self._is_between(r, start, end)]

    def _is_between(self, record, start, end):
        if record['metadata']['end'] < start:
            return False
        if record['metadata']['start'] > end:
            return False
        return True

    def _prepare_time_bucket_kwargs(self, bucket, what):
        i = str(bucket) + ':' + what
        return dict(
            KeyConditionExpression=Key('time_index_key').eq(i)
        )

    @memoized_property
    def _table(self):
        return self.dynamodb.Table(self.table_name)

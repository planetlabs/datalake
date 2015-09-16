from memoized_property import memoized_property
from boto3.dynamodb.conditions import Key

class ArchiveQuerier(object):

    def __init__(self, table_name, dynamodb=None):
        self.table_name = table_name
        self.dynamodb = dynamodb

    def query_by_work_id(self, work_id, what, where=None):
        i = work_id + ':' + what
        kwargs = dict(
            IndexName='work-id-index',
            KeyConditionExpression=Key('work_id_index_key').eq(i)
        )
        response = self._table.query(**kwargs)
        return response['Items']

    @memoized_property
    def _table(self):
        return self.dynamodb.Table(self.table_name)

import pytest
from datalake_common import DatalakeRecord
from datalake_common.tests import random_metadata

from datalake_api.querier import ArchiveQuerier


def test_query_work_id(table_maker, dynamodb):
    metadata = [random_metadata() for i in range(2)]
    metadata[0]['work_id'] = 'work0'
    metadata[1]['work_id'] = 'work1'
    records = []
    for m in metadata:
        url = 's3://' + m['work_id']
        records += DatalakeRecord.list_from_metadata(url, m)
    table = table_maker(records)
    
    aq = ArchiveQuerier('test', dynamodb=dynamodb)
    results = aq.query_by_work_id('work0', metadata[0]['what'])
    assert len(results) >= 1
    for r in results:
        assert r['metadata'] == metadata[0]
        assert r['url'] == 's3://work0'


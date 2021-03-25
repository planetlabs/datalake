import pytest
import responses
from datalake import DatalakeHttpError
from copy import copy
from datalake.common import Metadata
from datetime import datetime, timedelta
from pytz import utc
import simplejson as json
from conftest import prepare_response
import requests


@responses.activate
def test_list_one_page(archive, random_metadata):
    r = {
        'records': [
            {
                'url': 's3://bucket/file',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     start=random_metadata['start'],
                     end=random_metadata['end'])
    l = list(archive.list(random_metadata['what'],
                          start=random_metadata['start'],
                          end=random_metadata['end']))
    assert len(l) == 1
    assert l[0]['url'] == 's3://bucket/file'
    assert l[0]['metadata'] == random_metadata


@responses.activate
def test_list_two_pages(archive, random_metadata):
    m1 = copy(random_metadata)
    m1['id'] = '1'
    r1 = {
        'records': [
            {
                'url': 's3://bucket/file1',
                'metadata': m1,
            }
        ],
        'next': 'http://the-next-url/',
    }
    prepare_response(r1, what=random_metadata['what'], start=m1['start'],
                     end=m1['end'])

    m2 = copy(random_metadata)
    m2['id'] = '2'
    r2 = {
        'records': [
            {
                'url': 's3://bucket/file2',
                'metadata': m2,
            }
        ],
        'next': None,
    }
    prepare_response(r2, url='http://the-next-url/')
    l = list(archive.list(m1['what'],
                          start=random_metadata['start'],
                          end=random_metadata['end']))
    assert len(l) == 2
    assert l[0]['url'] == 's3://bucket/file1'
    assert l[0]['metadata'] == m1
    assert l[1]['url'] == 's3://bucket/file2'
    assert l[1]['metadata'] == m2


@responses.activate
def test_bad_request(archive):

    r = {
        "code": "NoWorkInterval",
        "message": "You must provide either work_id or start/end"
    }
    prepare_response(r, status=400, what='syslog')

    with pytest.raises(DatalakeHttpError):
        list(archive.list('syslog'))


@responses.activate
def test_internal_server_error(archive):

    r = 'INTERNAL SERVER ERROR'
    prepare_response(r, status=500, what='syslog')

    with pytest.raises(DatalakeHttpError):
        list(archive.list('syslog'))


@pytest.fixture
def date_tester(archive, random_metadata):

    def tester(start, end):
        random_metadata['start'] = Metadata.normalize_date(start)
        random_metadata['end'] = Metadata.normalize_date(end)
        r = {
            'records': [
                {
                    'url': 's3://bucket/file',
                    'metadata': random_metadata,
                }
            ],
            'next': None,
        }

        prepare_response(r, what=random_metadata['what'],
                         start=random_metadata['start'],
                         end=random_metadata['end'])
        l = list(archive.list(random_metadata['what'], start=start, end=end))
        assert len(l) == 1
        assert l[0]['url'] == 's3://bucket/file'
        assert l[0]['metadata'] == random_metadata

    return tester


@responses.activate
def test_datetime_date(date_tester):
    start = datetime.now(utc) - timedelta(days=1)
    end = datetime.now(utc)
    date_tester(start, end)


@responses.activate
def test_human_readable_date(date_tester):
    start = '1977-01-01'
    end = '1977-01-02'
    date_tester(start, end)


@responses.activate
def test_with_where(archive, random_metadata):
    r = {
        'records': [
            {
                'url': 's3://bucket/file',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     where=random_metadata['where'],
                     start=random_metadata['start'],
                     end=random_metadata['end'])
    l = list(archive.list(random_metadata['what'],
                          where=random_metadata['where'],
                          start=random_metadata['start'],
                          end=random_metadata['end']))
    assert len(l) == 1
    assert l[0]['url'] == 's3://bucket/file'
    assert l[0]['metadata'] == random_metadata


@responses.activate
def test_with_work_id(archive, random_metadata):
    random_metadata['work_id'] = 'foo123'

    r = {
        'records': [
            {
                'url': 's3://bucket/file',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     work_id=random_metadata['work_id'])
    l = list(archive.list(random_metadata['what'],
                          work_id='foo123'))
    assert len(l) == 1
    assert l[0]['url'] == 's3://bucket/file'
    assert l[0]['metadata'] == random_metadata


@responses.activate
def test_list_cli_url_format(cli_tester, random_metadata):
    r = {
        'records': [
            {
                'url': 's3://thisistheurl',
                'metadata': random_metadata,
            },
            {
                'url': 's3://thisistheotherurl',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     start=random_metadata['start'],
                     end=random_metadata['end'])
    cmd = 'list {what} --start={start} --end={end}'
    cmd = cmd.format(**random_metadata)
    output = cli_tester(cmd)
    assert output == 's3://thisistheurl\ns3://thisistheotherurl\n'


@responses.activate
def test_list_cli_json_format(cli_tester, random_metadata):
    m1 = copy(random_metadata)
    m1['id'] = '1'
    m1['work_id'] = 'foo1234'
    m2 = copy(random_metadata)
    m2['id'] = '2'
    m2['work_id'] = 'foo1234'
    r = {
        'records': [
            {
                'url': 's3://url1',
                'metadata': m1,
            },
            {
                'url': 's3://url2',
                'metadata': m2,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=m1['what'], work_id=m1['work_id'])
    cmd = 'list {what} --work-id={work_id} --format=json'
    cmd = cmd.format(**m1)
    output_lines = cli_tester(cmd).rstrip('\n').split('\n')
    assert len(output_lines) == 2
    output_jsons = [json.loads(l) for l in output_lines]
    for record in r['records']:
        assert record in output_jsons


@responses.activate
def test_list_cli_http_format(cli_tester, random_metadata):
    m1 = copy(random_metadata)
    m1['id'] = '1'
    m1['work_id'] = 'foo1234'
    m2 = copy(random_metadata)
    m2['id'] = '2'
    m2['work_id'] = 'foo1234'
    r = {
        'records': [
            {
                'url': 's3://url1',
                'http_url': 'https://foo.com/url1',
                'metadata': m1,
            },
            {
                'url': 's3://url2',
                'http_url': 'https://foo.com/url2',
                'metadata': m2,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=m1['what'], work_id=m1['work_id'])
    cmd = 'list {what} --work-id={work_id} --format=http'
    cmd = cmd.format(**m1)
    output = cli_tester(cmd).rstrip('\n').split('\n')
    assert output == ['https://foo.com/url1', 'https://foo.com/url2']


@responses.activate
def test_list_cli_human_format(cli_tester, random_metadata):
    m1 = copy(random_metadata)
    m1['id'] = '1'
    m1['work_id'] = 'foo1234'
    m1['start'] = 1612548642000
    m1['end'] = 1612548643000
    m2 = copy(random_metadata)
    m2['id'] = '2'
    m2['work_id'] = 'foo1234'
    m2['start'] = 1612548642000
    m2['end'] = 1612548643000
    r = {
        'records': [
            {
                'url': 's3://url1',
                'metadata': m1,
            },
            {
                'url': 's3://url2',
                'metadata': m2,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=m1['what'], work_id=m1['work_id'])
    cmd = 'list {what} --work-id={work_id} --format=human'
    cmd = cmd.format(**m1)
    stanzas = [s for s in cli_tester(cmd).split('\n\n') if s]

    for s in stanzas:
        lines = [l for l in s.split('\n')]
        # just check for the start/end
        assert 'start: 2021-02-05T18:10:42+00:00' in lines
        assert 'end: 2021-02-05T18:10:43+00:00' in lines
    print(stanzas)
    assert len(stanzas) == 2


@responses.activate
def test_list_cli_human_format_no_end_time(cli_tester, random_metadata):
    m1 = copy(random_metadata)
    m1['id'] = '1'
    m1['work_id'] = 'foo1234'
    m1['start'] = 1612548642000
    m1['end'] = None
    m2 = copy(random_metadata)
    m2['id'] = '2'
    m2['work_id'] = 'foo1234'
    m2['start'] = 1612548642000
    m2['end'] = None
    r = {
        'records': [
            {
                'url': 's3://url1',
                'metadata': m1,
            },
            {
                'url': 's3://url2',
                'metadata': m2,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=m1['what'], work_id=m1['work_id'])
    cmd = 'list {what} --work-id={work_id} --format=human'
    cmd = cmd.format(**m1)
    stanzas = [s for s in cli_tester(cmd).split('\n\n') if s]
    for s in stanzas:
        lines = [l for l in s.split('\n')]
        # just check for the start/end
        assert 'start: 2021-02-05T18:10:42+00:00' in lines
        assert 'end: null' in lines
    assert len(stanzas) == 2


TEST_REQUESTS = []


class SessionWrapper(requests.Session):

    def __init__(self, *args, **kwargs):
        global TEST_REQUESTS
        TEST_REQUESTS = []
        return super(SessionWrapper, self).__init__(*args, **kwargs)

    def request(self, method, url, **kwargs):
        global TEST_REQUESTS
        TEST_REQUESTS.append((method, url))
        return super(SessionWrapper, self).request(method, url, **kwargs)


@responses.activate
def test_list_with_injected_session(archive_maker, random_metadata):
    r = {
        'records': [
            {
                'url': 's3://bucket/file',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     start=random_metadata['start'],
                     end=random_metadata['end'])

    s = SessionWrapper()
    a = archive_maker(session=s)
    l = list(a.list(random_metadata['what'],
                    start=random_metadata['start'],
                    end=random_metadata['end']))
    assert len(TEST_REQUESTS) > 0
    assert len(l) > 0
    assert l[0]['url'] == 's3://bucket/file'
    assert l[0]['metadata'] == random_metadata


@responses.activate
def test_list_with_session_class(monkeypatch,
                                 archive_maker,
                                 random_metadata):
    r = {
        'records': [
            {
                'url': 's3://bucket/file',
                'metadata': random_metadata,
            }
        ],
        'next': None,
    }
    prepare_response(r, what=random_metadata['what'],
                     start=random_metadata['start'],
                     end=random_metadata['end'])

    monkeypatch.setenv('DATALAKE_SESSION_CLASS', 'test_list.SessionWrapper')
    a = archive_maker()
    l = list(a.list(random_metadata['what'],
                    start=random_metadata['start'],
                    end=random_metadata['end']))
    assert len(TEST_REQUESTS) > 0
    assert len(l) > 0
    assert l[0]['url'] == 's3://bucket/file'
    assert l[0]['metadata'] == random_metadata

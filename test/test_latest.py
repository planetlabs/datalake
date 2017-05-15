import pytest
import responses
from conftest import prepare_response
from datalake import DatalakeHttpError


@responses.activate
def test_latest(archive, random_metadata):
    r = {
        'url': 's3://bucket/file',
        'metadata': random_metadata,
    }
    url = 'http://datalake.example.com/v0/archive/latest/{}/{}'
    url = url.format(random_metadata['what'], random_metadata['where'])
    prepare_response(r, url=url)
    l = archive.latest(random_metadata['what'], random_metadata['where'])
    assert l['url'] == 's3://bucket/file'
    assert l['metadata'] == random_metadata


@responses.activate
def test_latest_cli(cli_tester, random_metadata):
    r = {
        'url': 's3://bucket/file',
        'metadata': random_metadata,
    }
    url = 'http://datalake.example.com/v0/archive/latest/{}/{}'
    url = url.format(random_metadata['what'], random_metadata['where'])
    prepare_response(r, url=url)

    cmd = 'latest {what} {where}'
    cmd = cmd.format(**random_metadata)
    output = cli_tester(cmd)
    assert output == 's3://bucket/file\n'


@responses.activate
def test_latest_with_lookback_cli(cli_tester, random_metadata):
    r = {
        'url': 's3://bucket/file',
        'metadata': random_metadata,
    }
    url = 'http://datalake.example.com/v0/archive/latest/{}/{}?lookback=42'
    url = url.format(random_metadata['what'], random_metadata['where'])
    prepare_response(r, url=url)

    cmd = 'latest {what} {where} --lookback 42'
    cmd = cmd.format(**random_metadata)
    output = cli_tester(cmd)
    assert output == 's3://bucket/file\n'


@responses.activate
def test_no_such_latest(archive):
    r = {
        'message': 'not found',
        'code': 'NoSuchDatalakeFile',
    }
    url = 'http://datalake.example.com/v0/archive/latest/not/here'
    prepare_response(r, status=404, url=url)
    with pytest.raises(DatalakeHttpError):
        archive.latest('not', 'here')


@responses.activate
def test_latest_cli_bad_lookback(cli_tester, random_metadata):
    cmd = 'latest foo bar --lookback nine'
    cli_tester(cmd, expected_exit=2)

import pytest
import os
from datalake.common.conf import load_config
from datalake.common.errors import InsufficientConfiguration


@pytest.fixture
def mockenv(monkeypatch):
    monkeypatch.setattr("os.environ", {})


def test_config_file(mockenv, tmpfile):
    config_vars = [
        'DATALAKE_FOO=bar',
    ]
    conf = tmpfile('\n'.join(config_vars))
    load_config(conf, None)
    assert 'DATALAKE_FOO' in os.environ
    assert os.environ['DATALAKE_FOO'] == 'bar'


def test_env_overrides_config(mockenv, tmpfile):
    os.environ['DATALAKE_FOO'] = 'baz'
    config_vars = [
        'DATALAKE_FOO=bar',
    ]
    conf = tmpfile('\n'.join(config_vars))
    load_config(conf, None)
    assert 'DATALAKE_FOO' in os.environ
    assert os.environ['DATALAKE_FOO'] == 'baz'


def test_kwarg_overrides_all(mockenv, tmpfile):
    os.environ['DATALAKE_FOO'] = 'baz'
    config_vars = [
        'DATALAKE_FOO=bar',
    ]
    conf = tmpfile('\n'.join(config_vars))
    load_config(conf, None, foo='bing')
    assert 'DATALAKE_FOO' in os.environ
    assert os.environ['DATALAKE_FOO'] == 'bing'


def test_aws_exception(monkeypatch, tmpfile):
    load_config(None, None, aws_variable='value')
    assert 'AWS_VARIABLE' in os.environ
    assert os.environ['AWS_VARIABLE'] == 'value'


def test_default_config(mockenv, tmpfile):
    default_config_vars = [
        'DATALAKE_FOO=bar',
    ]
    default_conf = tmpfile('\n'.join(default_config_vars))

    load_config(None, default_conf)
    assert 'DATALAKE_FOO' in os.environ
    assert os.environ['DATALAKE_FOO'] == 'bar'


def test_config_overrides_default_config(mockenv, tmpfile):
    default_config_vars = [
        'DATALAKE_FOO=bar',
    ]
    default_conf = tmpfile('\n'.join(default_config_vars))

    config_vars = [
        'DATALAKE_FOO=bing',
    ]
    conf = tmpfile('\n'.join(config_vars))

    load_config(conf, default_conf)
    assert 'DATALAKE_FOO' in os.environ
    assert os.environ['DATALAKE_FOO'] == 'bing'


def test_config_does_not_exist(mockenv, tmpfile):
    with pytest.raises(InsufficientConfiguration):
        load_config('/no/such/file', None)

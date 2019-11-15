import pytest
from datalake.common.errors import InsufficientConfiguration
from datalake import load_config
from datalake.tests import random_word
import os


def test_config_file_loads(tmpfile):
    var = random_word(8)
    f = tmpfile('{}=FOO'.format(var))
    load_config(config_file=f)
    assert var in os.environ
    assert os.environ[var] == 'FOO'


def test_config_file_loads_from_envvar(monkeypatch, tmpfile):
    var = random_word(8)
    f = tmpfile('{}=BAR'.format(var))
    monkeypatch.setenv('DATALAKE_CONFIG', f)
    load_config()
    assert var in os.environ
    assert os.environ[var] == 'BAR'


def test_no_such_config():
    with pytest.raises(InsufficientConfiguration):
        load_config(config_file='/no/such/config')


def test_no_such_config_envvar(monkeypatch):
    monkeypatch.setenv('DATALAKE_CONFIG', '/no/such/config')
    with pytest.raises(InsufficientConfiguration):
        load_config()

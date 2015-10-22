import pytest

from datalake import Translator, TranslatorError


def test_valid():
    t = Translator('.*job-(?P<job_id>[0-9]+).log$~job{job_id}')
    s = t.translate('/var/log/jobs/job-1234.log')
    assert s == 'job1234'

def test_extract_from_path():
    t = Translator('.*/(?P<server_name>.*)/job-[0-9]+.log$~{server_name}')
    s = t.translate('/var/log/jobs/myserver/job-1234.log')
    assert s == 'myserver'

def test_missing_tilde():
    with pytest.raises(TranslatorError):
        Translator('no-tilde-here')

def test_too_many_tildes():
    with pytest.raises(TranslatorError):
        Translator('here-a~there-a~')

def test_format_missing_right_brace():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+).log$~job{job_id')
        t.translate('/var/log/jobs/job-1234.log')

def test_format_missing_left_brace():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+).log$~jobjob_id}')
        t.translate('/var/log/jobs/job-1234.log')

def test_format_missing_named_group():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-([0-9]+).log$~job{job_id}')
        t.translate('/var/log/jobs/job-1234.log')

def test_invalid_group_name():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id[0-9]+).log$~job{job_id}')

def test_unbalanced_parenthesis():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+.log$~job{job_id}')

def test_extract_does_not_match():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+).log$~job{job_id}')
        t.translate('/var/log/jobs/foo-1234.log')

def test_unexpected_name_in_format():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+).log$~job{foo_id}')
        t.translate('/var/log/jobs/job-1234.log')

def test_not_absolute_path():
    with pytest.raises(TranslatorError):
        t = Translator('.*job-(?P<job_id>[0-9]+).log$~job{job_id}')
        t.translate('var/log/jobs/job-1234.log')

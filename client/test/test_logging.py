import pytest
from datalake_common.errors import InsufficientConfiguration

from datalake.logging_helpers import prepare_logging, sentry_available


@pytest.mark.skipif(sentry_available(), reason='requires no sentry')
def test_fail_on_bad_sentry_config(monkeypatch):
    fake_dsn = 'https://100:200@sentry.example.com/1234'
    monkeypatch.setenv('DATALAKE_SENTRY_DSN', fake_dsn)
    with pytest.raises(InsufficientConfiguration):
        prepare_logging()


@pytest.mark.skipif(not sentry_available(), reason='requires sentry')
def test_prepare_logging(monkeypatch):
    fake_dsn = 'https://100:200@sentry.example.com/1234'
    monkeypatch.setenv('DATALAKE_SENTRY_DSN', fake_dsn)
    prepare_logging()

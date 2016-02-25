import pytest
from datalake_ingester import S3ToDatalakeTranslator


def _import_exception(exception_name):
    mod_name = '.'.join(exception_name.split('.')[0:-1])
    exception_name = exception_name.split('.')[-1]
    mod = __import__(mod_name, fromlist=[exception_name])
    return getattr(mod, exception_name)


def test_s3_notifications(event_test_driver):

    def tester(event):
        t = S3ToDatalakeTranslator()
        if 'translator_exception' in event:
            exception = _import_exception(event['translator_exception'])
            with pytest.raises(exception):
                t.translate(event['s3_notification'])
        else:
            translated = t.translate(event['s3_notification'])
            assert translated == event['expected_datalake_records']

    event_test_driver(tester)

import pytest
from datalake_ingester import S3Notification


def _import_exception(exception_name):
    mod_name = '.'.join(exception_name.split('.')[0:-1])
    exception_name = exception_name.split('.')[-1]
    mod = __import__(mod_name, fromlist=[exception_name])
    return getattr(mod, exception_name)


def _get_records(s3_notification):

    n = S3Notification(s3_notification)
    records = []
    for e in n.events:
        records += e.datalake_records
    return records


def test_s3_notifications(event_test_driver):

    def tester(event):

        if 's3_notification_exception' in event:
            exception = _import_exception(event['s3_notification_exception'])
            with pytest.raises(exception):
                _get_records(event['s3_notification'])
        else:
            records = _get_records(event['s3_notification'])
            assert records == event['expected_datalake_records']

    event_test_driver(tester)

import pytest
import simplejson as json
import os
from glob import glob

from datalake_backend import S3ToDatalakeTranslator
from datalake_backend.errors import InvalidS3Notification, InvalidS3Event

from conftest import test_data_path


_spec_path = os.path.join(test_data_path, 's3-notification-*.json')
_specs = glob(_spec_path)


@pytest.fixture(params=_specs)
def s3_notification_tester(request, s3_file_from_record):
    spec = json.load(open(request.param))
    expected_records = spec['expected_datalake_records']
    [s3_file_from_record(d['url'], d['metadata']) for d in expected_records]
    t = S3ToDatalakeTranslator()
    translated = t.translate(spec['s3_notification'])
    assert translated == expected_records


def test_sample_s3_notifications(s3_notification_tester):
    # test all the good notifications
    pass


@pytest.fixture
def bad_notification_tester():

    def tester(spec, exception):
        f = os.path.join(test_data_path, spec)
        n = json.load(open(f))
        t = S3ToDatalakeTranslator()
        with pytest.raises(exception):
            t.translate(n)
    return tester


def test_no_message_raises_exception(bad_notification_tester):
    spec = 'bad-s3-notification-no-message.json'
    bad_notification_tester(spec, InvalidS3Notification)


def test_no_event_version_raises_exception(bad_notification_tester):
    spec = 'bad-s3-notification-no-event-version.json'
    bad_notification_tester(spec, InvalidS3Event)


def test_unsuppored_event_version_raises_exception(bad_notification_tester):
    spec = 'bad-s3-notification-unsupported-event-version.json'
    bad_notification_tester(spec, InvalidS3Event)


def test_no_metadata(s3_file_from_record):
    f = os.path.join(test_data_path, 's3-notification-one-record.json')
    spec = json.load(open(f))
    record = spec['expected_datalake_records'][0]
    s3_file_from_record(record['url'], None)
    t = S3ToDatalakeTranslator()
    with pytest.raises(InvalidS3Event):
        t.translate(spec['s3_notification'])

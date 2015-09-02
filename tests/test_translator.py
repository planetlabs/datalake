import pytest
import simplejson as json
import os

from datalake_backend import S3ToDatalakeTranslator
from datalake_backend.errors import InvalidS3Notification, InvalidS3Event
from datalake_common import InvalidDatalakeMetadata


from conftest import test_data_path, all_s3_notification_specs


@pytest.fixture(params=all_s3_notification_specs)
def s3_notification_tester(request, spec_maker):
    spec = spec_maker(request.param)
    t = S3ToDatalakeTranslator()
    translated = t.translate(spec['s3_notification'])
    assert translated == spec['expected_datalake_records']


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
    with pytest.raises(InvalidDatalakeMetadata):
        t.translate(spec['s3_notification'])

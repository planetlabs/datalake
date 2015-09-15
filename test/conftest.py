import pytest
import random
import string
from datetime import datetime, timedelta
from moto import mock_s3
import boto


@pytest.fixture
def s3_conn(request):
    mock = mock_s3()
    mock.start()
    conn = boto.connect_s3()

    def tear_down():
        mock.stop()
    request.addfinalizer(tear_down)

    return conn

BUCKET_NAME = 'datalake-test'

@pytest.fixture
def s3_bucket(s3_conn):
    return s3_conn.create_bucket(BUCKET_NAME)

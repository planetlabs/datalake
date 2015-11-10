# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import pytest
import json
from threading import Timer
import os
from datalake_common.tests import random_word

from datalake import Enqueuer, Uploader


@pytest.fixture
def queue_dir(tmpdir):
    d = os.path.join(str(tmpdir), 'queue')
    os.mkdir(d)
    return d


@pytest.fixture
def enqueuer(queue_dir):
    return Enqueuer(queue_dir)


@pytest.fixture
def uploader(archive, queue_dir):
    return Uploader(archive, queue_dir)


@pytest.fixture
def uploaded_file_validator(archive, s3_key):

    def validator(f):
        expected_content = f.read()
        url = archive.url_from_file(f)
        from_s3 = s3_key(url)
        assert from_s3 is not None
        assert from_s3.get_contents_as_string() == expected_content
        metadata = json.loads(from_s3.get_metadata('datalake'))
        assert metadata == f.metadata

    return validator


@pytest.fixture
def random_file(tmpfile, random_metadata):
    expected_content = random_word(100)
    return tmpfile(expected_content)


def test_upload_existing(enqueuer, uploader, random_file, random_metadata,
                         uploaded_file_validator):
    f = enqueuer.enqueue(random_file, **random_metadata)
    uploader.listen(timeout=0.1)
    uploaded_file_validator(f)


def test_upload_incoming(enqueuer, uploader, random_file, random_metadata,
                         uploaded_file_validator):

    enqueued_files = []

    def enqueue():
        f = enqueuer.enqueue(random_file, **random_metadata)
        enqueued_files.append(f)

    t = Timer(0.5, enqueue)
    t.start()
    uploader.listen(timeout=1.0)

    for f in enqueued_files:
        uploaded_file_validator(f)

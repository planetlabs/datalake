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
from datalake_common.errors import InsufficientConfiguration
from datalake import Enqueuer, Uploader
from datalake.queue import has_queue
from conftest import crtime_setuid


@pytest.fixture
def queue_dir(monkeypatch, tmpdir):
    d = os.path.join(str(tmpdir), 'queue')
    os.mkdir(d)
    monkeypatch.setenv('DATALAKE_QUEUE_DIR', d)
    return d


@pytest.fixture
def enqueuer(queue_dir):
    return Enqueuer(queue_dir)


@pytest.fixture
def uploader(archive, queue_dir):
    return Uploader(archive, queue_dir)


@pytest.fixture
def uploaded_content_validator(s3_key):

    def validator(expected_content, expected_metadata=None):

        from_s3 = s3_key()
        assert from_s3 is not None
        assert from_s3.get_contents_as_string() == expected_content
        if expected_metadata is not None:
            metadata = json.loads(from_s3.get_metadata('datalake'))
            assert metadata == expected_metadata

    return validator


@pytest.fixture
def uploaded_file_validator(archive, uploaded_content_validator):

    def validator(f):
        expected_content = f.read()
        uploaded_content_validator(expected_content, f.metadata)

    return validator


@pytest.fixture
def random_file(tmpfile, random_metadata):
    expected_content = random_word(100)
    return tmpfile(expected_content)


@pytest.mark.skipif(not has_queue, reason='requires queuable features')
def test_upload_existing(enqueuer, uploader, random_file, random_metadata,
                         uploaded_file_validator):
    f = enqueuer.enqueue(random_file, **random_metadata)
    uploader.listen(timeout=0.1)
    uploaded_file_validator(f)


@pytest.mark.skipif(not has_queue, reason='requires queuable features')
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


@pytest.mark.skipif(not has_queue, reason='requires queuable features')
def test_upload_existing_cli(cli_tester, random_file, random_metadata,
                             uploaded_content_validator, queue_dir):
    cmd = 'enqueue --start={start} --end={end} --where {where} '
    cmd += '--what {what} '
    if random_metadata.get('work_id'):
        cmd += '--work-id {work_id} '
    cmd = cmd.format(**random_metadata)
    cli_tester(cmd + random_file)
    cmd = 'uploader --timeout=0.1'
    cli_tester(cmd)

    expected_content = open(random_file, 'rb').read()
    uploaded_content_validator(expected_content)


@pytest.mark.skipif(not has_queue or not crtime_setuid,
                    reason='requires queuable features and crtime')
def test_enqueue_with_crtime_and_now(cli_tester, random_file, random_metadata,
                                     uploaded_content_validator, queue_dir):
    cmd = 'enqueue --start=crtime --end=now --where server37 '
    cmd += '--what randomefile '
    cli_tester(cmd + random_file)


@pytest.mark.skipif(has_queue, reason='requires queuable to be not installed')
def test_uploader_queable_not_installed(archive, queue_dir):
    with pytest.raises(InsufficientConfiguration):
        Uploader(archive, queue_dir)


@pytest.mark.skipif(has_queue, reason='requires queuable to be not installed')
def test_enqueuer_queable_not_installed(queue_dir):
    with pytest.raises(InsufficientConfiguration):
        Enqueuer(queue_dir)

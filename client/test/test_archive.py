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

import simplejson as json
import re


def test_push_file(archive, random_metadata, tmpfile, s3_key):
    expected_content = 'mwahaha'.encode('utf-8')
    f = tmpfile(expected_content)
    url = archive.prepare_metadata_and_push(f, **random_metadata)
    from_s3 = s3_key(url)
    assert from_s3.get_contents_as_string() == expected_content
    metadata = from_s3.get_metadata('datalake')
    assert metadata is not None
    metadata = json.loads(metadata)
    common_keys = set(metadata.keys()).intersection(random_metadata.keys())
    assert common_keys == set(random_metadata.keys())


def test_file_url(archive, random_metadata, tmpfile):
    expected_content = 'mwahaha'
    f = tmpfile(expected_content)
    url = archive.prepare_metadata_and_push(f, **random_metadata)
    assert bool(re.match(r'^s3://datalake-test/[a-z0-9]{40}/data$', url))

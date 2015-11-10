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
from click.testing import CliRunner
from datalake.scripts.cli import cli
import os
from test_crtime import crtime_setuid


@pytest.fixture
def cli_tester(s3_bucket):

    def tester(command, expected_exit=0):
        os.environ['DATALAKE_STORAGE_URL'] = 's3://datalake-test'
        parts = command.split(' ')
        runner = CliRunner()
        result = runner.invoke(cli, parts, catch_exceptions=False)
        assert result.exit_code == expected_exit, result.output

    return tester


def test_cli_without_command_fails(cli_tester):
    cli_tester('', expected_exit=2)


def test_cli_with_version_succeeds(cli_tester):
    cli_tester('--version')


def test_push_with_metadata(cli_tester, tmpfile):
    cmd = ('push --start=2015-05-15 --end=2015-05-16 --where box --what log '
           '--data-version 0 ')
    cli_tester(cmd + tmpfile(''))


def test_push_without_end(cli_tester, tmpfile):
    cmd = ('push --data-version 0 --start=2015-05-15 --where=cron '
           '--what=report ' + tmpfile(''))
    cli_tester(cmd)


def test_push_with_aws_vars(cli_tester, tmpfile):
    cmd = ('-k abcd -s 1234 -r us-gov-west-1 push --data-version 0 '
           '--start=2015-09-14 --where=cron --what=report ' + tmpfile(''))
    cli_tester(cmd)


def test_push_with_config_file(cli_tester, tmpfile):
    cfg_json = '''{"aws_key": "abcd",
                   "aws_secret": "1234",
                   "aws_region": "us-gov-west-1"}'''
    cfg = tmpfile(cfg_json)
    cmd = ('-c ' + cfg + ' push --data-version 0 --start=2015-09-14 '
           '--where=cron --what=report /dev/null')
    cli_tester(cmd)


def test_translate_with_bad_expression_fails(cli_tester):
    cli_tester('translate foo bar', expected_exit=2)


def test_translate_with_good_args_succceeds(cli_tester):
    cmd = ("translate .*job-(?P<job_id>[0-9]+).log$~job{job_id} "
           "/var/log/job-456.log")
    print cmd
    cli_tester(cmd)


@pytest.mark.skipif(not crtime_setuid, reason='crtime required')
def test_crtime_and_now(cli_tester, tmpfile):
    f = tmpfile('contents')
    cmd = 'push --start=crtime --where=server123 '
    cmd += '--what=test --data-version 0 --end=now ' + f
    cli_tester(cmd)


def test_push_with_default_where(monkeypatch, cli_tester, tmpfile):
    monkeypatch.setenv('DATALAKE_DEFAULT_WHERE', 'hostname')
    cmd = ('push --start=2015-05-15 --end=2015-05-16 --what log '
           '--data-version 0 ')
    cli_tester(cmd + tmpfile(''))


def test_push_with_translation_expression(cli_tester, tmpdir):
    f = tmpdir.join('job-1234.log')
    f.write('blaaaa')
    cmd = 'push --work-id=.*job-(?P<job_id>[0-9]+).log$~job{job_id} '
    cmd += '--what=job --start=now --end=now --where=hostname '
    cmd += '--data-version 0 ' + str(f)
    cli_tester(cmd)

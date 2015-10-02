import pytest
from click.testing import CliRunner
from datalake.scripts.cli import cli
import os
from datalake_common.tests import tmpfile


@pytest.fixture
def cli_tester(s3_bucket):

    def tester(command, expected_exit=0):
        os.environ['DATALAKE_STORAGE_URL'] = 's3://datalake-test'
        parts = command.split(' ')
        runner = CliRunner()
        result = runner.invoke(cli, parts, catch_exceptions=False)
        assert result.exit_code == expected_exit

    return tester


def test_cli_without_command_fails(cli_tester):
    cli_tester('', expected_exit=2)


def test_cli_with_version_succeeds(cli_tester):
    cli_tester('--version')


def test_push_with_metadata(cli_tester, tmpfile):
    cmd = 'push --start=2015-05-15 --end=2015-05-16 --where box --what log '
    cli_tester(cmd + tmpfile(''))


def test_push_without_end(cli_tester, tmpfile):
    cmd = 'push --start=2015-05-15 --where=cron --what=report ' + tmpfile('')
    cli_tester(cmd)


def test_push_with_aws_vars(cli_tester, tmpfile):
    cmd = '-k abcd -s 1234 -r us-gov-west-1 '
    cmd += 'push --start=2015-09-14 --where=cron --what=report ' + tmpfile('')
    cli_tester(cmd)


def test_push_with_config_file(cli_tester, tmpfile):
    cfg_json = '''{"aws_key": "abcd",
                   "aws_secret": "1234",
                   "aws_region": "us-gov-west-1"}'''
    cfg = tmpfile(cfg_json)
    cmd = '-c ' + cfg
    cmd += ' push --start=2015-09-14 --where=cron --what=report /dev/null'
    cli_tester(cmd)

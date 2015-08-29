import pytest
from click.testing import CliRunner
from datalake.scripts.cli import cli
import os


@pytest.fixture
def cli_tester(s3_bucket):

    def tester(command, expected_exit=0):
        os.environ['DL_STORAGE_URL'] = 's3://datalake-test'
        parts = command.split(' ')
        runner = CliRunner()
        result = runner.invoke(cli, parts)
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

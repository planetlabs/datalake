from click.testing import CliRunner
from datalake.scripts.cli import cli


def test_cli_without_command_fails():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code != 0


def test_cli_with_version_succeeds():
    runner = CliRunner()
    result = runner.invoke(cli, ['--version'])
    assert result.exit_code == 0

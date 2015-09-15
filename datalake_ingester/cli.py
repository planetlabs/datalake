import click
import simplejson as json
import os
from conf import set_config

from ingester import Ingester

DEFAULT_CONFIG = '/etc/datalake-ingester.json'

@click.group(invoke_without_command=True)
@click.version_option()
@click.option('-c', '--config',
              help=('config file. The format is just a flat json with key '
                    'names that you can guess'))
@click.option('-t', '--dynamodb-table',
              help='dynamodb table in which to store datalake records.')
@click.option('-r', '--aws-region',
              help='region to use for aws services (e.g., s3, dynamodb)')
@click.option('-k', '--report-key',
              help='key under which reports should be published.')
@click.option('-s', '--s3-host',
              help='s3 host (e.g., s3-us-gov-west-1.amazonaws.com)')
@click.option('-q', '--queue',
              help='name of the ingestion queue (e.g., datalake-sqs)')
@click.option('--catch-exceptions',
              help='log exceptions and move on')
@click.pass_context
def cli(ctx, **kwargs):
    conf = _read_config_file(kwargs.pop('config'))
    conf.update({k: v for k, v in kwargs.iteritems() if v is not None})
    set_config(conf)

    _subcommand_or_fail(ctx)


def _read_config_file(config):
    if config is None:
        if os.path.exists(DEFAULT_CONFIG):
            return json.load(open(DEFAULT_CONFIG))
        else:
            return {}
    elif os.path.exists(config):
        return json.load(open(config))
    else:
        raise click.UsageError('Config file {} not exist'.format(config))


def _subcommand_or_fail(ctx):
    if ctx.invoked_subcommand is None:
        ctx.fail('Please specify a command.')


@cli.command()
def listen():
    i = Ingester.from_config()
    i.listen()

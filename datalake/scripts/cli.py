import click
from datalake import Archive
from datalake.conf import set_config
import os
import simplejson as json


DEFAULT_CONFIG = '/etc/datalake.json'

archive = None

@click.group(invoke_without_command=True)
@click.version_option()
@click.pass_context
@click.option('-c', '--config',
              help=('config file. The format is just a flat json with key '
                    'names that you can guess'))
@click.option('-u', '--storage-url',
              help=('The URL to the top-level storage resource where '
                    'datalake will archive all the files.'),
              envvar='DL_STORAGE_URL')
@click.option('-k', '--aws-key',
              help='The AWS access key used to read and write s3.',
              envvar='AWS_ACCESS_KEY_ID')
@click.option('-s', '--aws-secret',
              help='The AWS secret key used to read and write s3.',
              envvar='AWS_SECRET_ACCESS_KEY')
@click.option('-r', '--aws-region',
              help='The AWS region where files should be stored.',
              envvar='AWS_REGION')
def cli(ctx, **kwargs):
    conf = _read_config_file(kwargs.pop('config'))
    conf.update({k: v for k, v in kwargs.iteritems() if v is not None})
    set_config(conf)
    _subcommand_or_fail(ctx)
    _prepare_archive_or_fail(ctx, kwargs.pop('storage_url'))


def _read_config_file(config):
    if config is None:
        if os.path.exists(DEFAULT_CONFIG):
            return json.load(open(DEFAULT_CONFIG))
        else:
            return {}
    elif os.path.exists(config):
        print config
        print open(config).read()
        return json.load(open(config))
    else:
        raise click.UsageError('Config file {} not exist'.format(config))


def _subcommand_or_fail(ctx):
    if ctx.invoked_subcommand is None:
        ctx.fail('Please specify a command.')


def _prepare_archive_or_fail(ctx, storage_url):
    global archive
    archive = Archive(storage_url=storage_url)


# So, I fully appreciate the dissonance of "required options." But we have lots
# of required arguments, and I want to allow them in any order and I want their
# self-documenting feature.
@cli.command()
@click.option('--start')
@click.option('--end')
@click.option('--where')
@click.option('--what')
@click.option('--data-version')
@click.option('--work-id')
@click.argument('file')
def push(**kwargs):
    filename = kwargs.pop('file')
    url = archive.push(filename, **kwargs)
    click.echo('Pushed {} to {}'.format(filename, url))

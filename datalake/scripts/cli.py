import click
from datalake import Archive, Translator, TranslatorError
import os
import simplejson as json
from dotenv import load_dotenv

from datalake_common.metadata import InvalidDatalakeMetadata


DEFAULT_CONFIG = '/etc/datalake.env'

archive = None

def clean_up_datalake_errors(f):
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (InvalidDatalakeMetadata, TranslatorError) as e:
            raise click.UsageError(e.message)
    return wrapped


@click.group(invoke_without_command=True)
@click.version_option()
@click.pass_context
@click.option('-c', '--config',
              help=('config file. The format is just single lines with '
                    'VAR=VALUE. If ' + DEFAULT_CONFIG + ' exists it will '
                    'be read. Config file values can be overridden by '
                    'environment variables, which can be overridded by '
                    'command-line arguments.'))
@click.option('-u', '--storage-url',
              help=('The URL to the top-level storage resource where '
                    'datalake will archive all the files (e.g., '
                    's3://my-datalake). DATALAKE_STORAGE_URL is the '
                    'config/environment variable'),
              envvar='DATALAKE_STORAGE_URL')
@click.option('-k', '--aws-access-key-id',
              help=('The AWS access key used to read and write s3. '
                    'AWS_ACCESS_KEY_ID is the configuration/environment '
                    'variable.'),
              envvar='AWS_ACCESS_KEY_ID')
@click.option('-s', '--aws-secret-access-key',
              help=('The AWS secret key used to read and write s3.'
                    'AWS_SECRET_ACCESS_KEY is the configuration/environment '
                    'variable.'),
              envvar='AWS_SECRET_ACCESS_KEY')
@click.option('-r', '--aws-region',
              help=('The AWS region where files should be stored. '
                    'AWS_REGION is the configuration/environment '
                    'variable.'),
              envvar='AWS_REGION')
def cli(ctx, **kwargs):
    _read_config_file(kwargs.pop('config'))
    _update_environment(**kwargs)
    _subcommand_or_fail(ctx)


def _update_environment(**kwargs):
    for k, v in kwargs.iteritems():
        if v is None:
            continue
        if not k.startswith('aws'):
            k = 'DATALAKE_' + k
        k = k.upper()
        os.environ[k] = v


def _read_config_file(config):
    if config is None:
        if os.path.exists(DEFAULT_CONFIG):
            load_dotenv(DEFAULT_CONFIG)
    elif os.path.exists(config):
        load_dotenv(config)
    else:
        raise click.UsageError('Config file {} not exist'.format(config))


def _subcommand_or_fail(ctx):
    if ctx.invoked_subcommand is None:
        ctx.fail('Please specify a command.')


def _prepare_archive_or_fail():
    global archive
    archive = Archive()


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
    _prepare_archive_or_fail()
    _push(**kwargs)


@clean_up_datalake_errors
def _push(**kwargs):
    filename = kwargs.pop('file')
    url = archive.prepare_metadata_and_push(filename, **kwargs)
    click.echo('Pushed {} to {}'.format(filename, url))


@cli.command()
@click.argument('translation-expression')
@click.argument('file')
def translate(**kwargs):
    _translate(**kwargs)

@clean_up_datalake_errors
def _translate(**kwargs):
    t = Translator(kwargs['translation_expression'])
    click.echo(t.translate(kwargs['file']))

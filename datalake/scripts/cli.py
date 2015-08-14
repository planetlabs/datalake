import click
from datalake import File, Archive


archive = None


@click.group(invoke_without_command=True)
@click.version_option()
@click.pass_context
@click.option('--storage-url')
def cli(ctx, **kwargs):
    _subcommand_or_fail(ctx)
    _prepare_archive_or_fail(ctx, kwargs.pop('storage_url'))


def _subcommand_or_fail(ctx):
    if ctx.invoked_subcommand is None:
        ctx.fail('Please specify a command.')


def _prepare_archive_or_fail(ctx, storage_url):
    global archive
    if storage_url is None:
        ctx.fail('You must specify --storage-url')
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
@click.argument('file')
def push(**kwargs):
    filename = kwargs.pop('file')
    f = File(filename, kwargs)
    url = archive.push(f)
    click.echo('Pushed {} to {}'.format(filename, url))

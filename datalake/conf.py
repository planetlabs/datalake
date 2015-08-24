from configargparse import ArgParser
from copy import deepcopy
import click


'''datalake configuration

A number of configuration variables influence the behavior of datalake. This is the
one place where configuration variables should be declared and
described. Configuration can conveniently come from environment variables or
config files.
'''

config_schema = {
    'storage_url': {
        'args': ['-u', '--storage-url'],
        'kwargs': dict(
            help=('The URL to the top-level storage resource where '
                  'datalake will archive all the files.'),
            env_var='DL_STORAGE_URL'
        )
    },
    'aws_key': {
        'args': ['-k', '--aws-key'],
        'kwargs': dict(
            help='The AWS access key used to read and write s3.',
            env_var='AWS_ACCESS_KEY_ID'
        )
    },
    'aws_secret': {
        'args': ['-s', '--aws-secret'],
        'kwargs': dict(
            help=('The AWS secret key used to read and write s3.'),
            env_var='AWS_SECRET_ACCESS_KEY'
        )
    },
    'aws_region': {
        'args': ['-r', '--aws-region'],
        'kwargs': dict(
            help=('The AWS region where files should be stored.'),
            env_var='AWS_REGION'
        )
    },
}


def get_click_option_args(option_name):
    option = deepcopy(config_schema[option_name])
    option['kwargs']['envvar'] = option['kwargs'].pop('env_var', None)
    return option


class datalake_click_option(object):
    '''just like @click.option, except read our custom option schema

    This simply allows interoperation with configargparse so we can get command
    line, ENV_VAR, and config file support all in one place.
    '''
    def __init__(self, name):
        self.name = name

    def __call__(self, f):
        option = get_click_option_args(self.name)
        @click.option(*option['args'], **option['kwargs'])
        def wrapped_f(*args, **kwargs):
            f(*args, **kwargs)
        return wrapped_f


config_parser = ArgParser(default_config_files=['/etc/datalake.conf'])

for v in config_schema.values():
    config_parser.add(*v['args'], **v['kwargs'])

_config = config_parser.parse_args(args=[])

def get_config():
    return _config

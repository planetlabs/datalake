from configargparse import ArgParser

'''atl configuration

A number of configuration variables influence the behavior of atl. This is the
one place where configuration variables should be declared and
described. Configuration can conveniently come from environment variables or
config files.
'''
config_parser = ArgParser(default_config_files=['/etc/atl.conf'])

DEFAULT_STORAGE_URL = 's3://allthelogs-test'
config_parser.add('-u', '--storage-url',
                  help=('The URL to the top-level storage resource where atl '
                        'will archive all the logs.'),
                  env_var='STORAGE_URL',
                  default=DEFAULT_STORAGE_URL)

config_parser.add('-a', '--aws-key',
                  help=('The AWS access key used to read and write s3.'),
                  env_var='AWS_ACCESS_KEY_ID')

config_parser.add('-a', '--aws-secret',
                  help=('The AWS secret key used to read and write s3.'),
                  env_var='AWS_SECRET_ACCESS_KEY')

config_parser.add('-a', '--aws-region',
                  help=('The AWS region where logs should be stored.'),
                  env_var='AWS_REGION')

_config = config_parser.parse_args(args=[])

def set_config(c):
    '''set the configuration

    This is generally only something that a command-line utility might do if it
    wants to parse command line arugments using the
    config_parser. Configuration variables are read at runtime. So the
    configuration should be set before any objects are created.
    '''
    global _config
    _config = c

def get_config():
    return _config

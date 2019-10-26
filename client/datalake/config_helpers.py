from dotenv import load_dotenv
import os
from datalake_common.errors import InsufficientConfiguration


DEFAULT_CONFIG = '/etc/datalake.env'


def load_config(config_file=None):
    '''load the datalake configuration from known places

    Configuration variables set in the environment are taken with higher
    precedence than those that appear in the config file.

    Args:

    config_file: The path to a configuration file. If None, one of two things
    happen in order of descending precedence:

                 1. If the DATALAKE_CONFIG environment variable is set, the
                    configuration file to which it points will be
                    loaded.

                 2. If the default configuration file exists, it will be
                    loaded.

    '''
    _read_config_file(config_file)


def _read_config_file(config_file):
    if config_file is None:
        config_file = os.environ.get('DATALAKE_CONFIG', DEFAULT_CONFIG)
    if os.path.exists(config_file):
        load_dotenv(config_file)
    elif config_file != DEFAULT_CONFIG:
        msg = 'Config file {} not exist.'.format(config_file)
        raise InsufficientConfiguration(msg)

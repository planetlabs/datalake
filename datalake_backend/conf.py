import configargparse

CONFIG_FILE = '/etc/datalake-backend.conf'

config_parser = configargparse.ArgParser(default_config_files=[CONFIG_FILE])

config_parser.add('-t', '--dynamodb-table',
                  help=('dynamodb table in which to store datalake records'),
                  env_var='DL_DYNAMODB_TABLE')

config_parser.add('-r', '--aws-region',
                  help=('region to use for aws services (e.g., s3, dynamodb)'),
                  env_var='DL_AWS_REGION')

config_parser.add('-k', '--report-key',
                  help=('key under which reports should be published. '
                        'This is the ARN for SNS topics.'),
                  env_var='DL_REPORT_KEY')

config_parser.add('-s', '--s3-host',
                  help=('s3 host to connect to (e.g., '
                        's3-us-gov-west-1.amazonaws.com)'),
                  env_var='DL_S3_HOST')

config_parser.add('-q', '--queue',
                  help=('name of the ingestion queue (e.g., sqs)'),
                  env_var='DL_QUEUE')

config = config_parser.parse_args(args=[])

def get_config():
    return config

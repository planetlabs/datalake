import boto3
import simplejson as json
import logging
from memoized_property import memoized_property
import os


class SNSReporter(object):
    '''report ingestion events to SNS'''

    def __init__(self, report_key):
        self.report_key = report_key
        self.logger = logging.getLogger(self._log_name)

    @classmethod
    def from_config(cls):
        report_key = os.environ.get('DATALAKE_REPORT_KEY')
        if report_key is None:
            return None
        return cls(report_key)

    @property
    def _log_name(self):
        return self.report_key.split(':')[-1]

    @memoized_property
    def _connection(self):
        region = os.environ.get('AWS_REGION', 'us-east-1')
        return boto3.client('sns', region_name=region)

    def report(self, ingestion_report):
        message = json.dumps(ingestion_report)
        self.logger.info('REPORTING: %s', message)
        self._connection.publish(TopicArn=self.report_key, Message=message)

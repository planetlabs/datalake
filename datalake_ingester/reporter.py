import boto.sns
import simplejson as json
import logging
from memoized_property import memoized_property
import os
from datalake_common.errors import InsufficientConfiguration


class SNSReporter(object):
    '''report ingestion events to SNS'''

    def __init__(self, report_key):
        self.report_key = report_key
        self.logger = logging.getLogger(self._log_name)

    @classmethod
    def from_config(cls):
        report_key = os.environ.get('DATALAKE_REPORT_KEY')
        if report_key is None:
            raise InsufficientConfiguration('Please configure a report_key')
        return cls(report_key)

    @property
    def _log_name(self):
        return self.report_key.split(':')[-1]

    @memoized_property
    def _connection(self):
        region = os.environ.get('AWS_REGION')
        if region:
            return boto.sns.connect_to_region(region)
        else:
            return boto.connect_sns()

    def report(self, ingestion_report):
        message = json.dumps(ingestion_report)
        self.logger.info('REPORTING: %s', message)
        self._connection.publish(topic=self.report_key, message=message)

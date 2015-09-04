import boto.sns
import simplejson as json
import logging

from conf import get_config
from errors import InsufficientConfiguration


class SNSReporter(object):
    '''report ingestion events to SNS'''

    def __init__(self, report_key):
        self.report_key = report_key
        self._prepare_connection()
        self.logger = logging.getLogger(self._log_name)

    @property
    def _log_name(self):
        return self.report_key.split(':')[-1]

    def _prepare_connection(self):
        region = get_config().aws_region
        self._connection = boto.connect_sns(region=region)

    def report(self, ingestion_report):
        message = json.dumps(ingestion_report)
        self.logger.info('REPORTING: %s', message)
        self._connection.publish(topic=self.report_key, message=message)

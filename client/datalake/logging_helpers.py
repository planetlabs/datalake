'''helper facilities for logging

The datalake client does not generally configure its own logging. But the
command line client may choose to configure logging in some cases. Users with
sentry accounts may wish to configure it by installing the sentry extras.
'''
import os
import logging
from datalake_common.errors import InsufficientConfiguration


def sentry_available():
    try:
        import raven.handlers.logging
        return hasattr(raven.handlers.logging, 'SentryHandler')
    except ImportError:
        return False


def _get_sentry_handler():

    dsn = os.environ.get('DATALAKE_SENTRY_DSN')
    if not dsn:
        return None

    if not sentry_available():
        msg = 'DATALAKE_SENTRY_DSN is configured but raven is not installed. '
        msg += '`pip install datalake[sentry]` to turn this feature on.'
        raise InsufficientConfiguration(msg)

    return {
        'level': 'ERROR',
        'class': 'raven.handlers.logging.SentryHandler',
        'dsn': dsn
    }


def prepare_logging():

    conf = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s %(name)s %(levelname)s %(message)s',
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout'
            },
        },
        'root': {
            'level': 'INFO',
            'handlers': ['console']
        }
    }

    sentry_handler = _get_sentry_handler()
    if sentry_handler:
        conf['handlers']['sentry'] = sentry_handler
        conf['root']['handlers'].append('sentry')

    logging.config.dictConfig(conf)

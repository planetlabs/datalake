import logging
import os


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
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'sentry': {
            'level': 'ERROR',
            'class': 'raven.handlers.logging.SentryHandler',
            'dsn': os.environ.get('SENTRY_APP_URL')
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

if os.environ.get('SENTRY_APP_URL'):
    conf['root']['handlers'].append('sentry')

logging.config.dictConfig(conf)

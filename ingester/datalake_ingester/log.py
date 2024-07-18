import logging
import sentry_sdk


_log_configured = False

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
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

def configure_logging():
    global _log_configured
    if not _log_configured:
        sentry_sdk.init()
        if conf:
            logging.config.dictConfig(conf)
        logging.debug(f"Logging initialized with provided conf {conf}.")
        _log_configured = True



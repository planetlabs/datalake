import logging
import sentry_sdk


def log_debugger(logger=None, message='', loc='', conf=None):
    """
    Initializes logging configuration and logs messages at different levels.

    Parameters:
        logger (logging.Logger): The logger instance to use for logging.
        message (str): The message to log.
        loc (str, optional): The location information (e.g., file or class). Defaults to ''.
        conf (dict, optional): The logging configuration dictionary.
    """
    sentry_sdk.init()
    if conf:
        logging.config.dictConfig(conf)
    logging.debug("Logging has been initialized with the provided configuration.")

    print(f'\n======= Inside {loc} log_debugger print: at {message} =======')

    if logger:
        # Log at different levels
        logger.info(f"======= Inside {loc} log_debugger logger.info: {message} =======\n")
        logger.warning(f"======= Inside {loc} log_debugger logger.warning: {message} =======\n")


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
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

log_debugger(conf=conf)

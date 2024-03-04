from sentry_sdk import Hub, start_transaction
import functools


class SentryTransaction:
    '''
    Context manager for performance monitoring in sentry. Used by the
    monitor_performance decorator. You can also use this to track any
    block of code. Example:

    with sentry.SentryTransaction(op='MetisCaptureCalculator'):
        # tracked code here
    '''
    def __init__(self, name=None, tags=None, transaction=None, op='default'):
        self.op = op
        self.name = name
        self.tags = tags
        self.hub = Hub.current
        self.transaction = transaction or self.hub.scope.transaction
        self.span = self.hub.scope.span
        self.new_transaction = None

    def __enter__(self):
        if self.transaction is None:
            self.new_transaction = start_transaction(
                name=self.name, op=self.op)
        else:
            self.new_transaction = self.span.start_child(
                op=self.op, hub=self.hub
            )
        if self.tags:
            for key, value in self.tags.items():
                self.new_transaction.set_tag(key, value)

    def __exit__(self, type, value, traceback):
        self.new_transaction.finish()


def monitor_performance(op=None):
    '''
    Use @monitor_performance() decorator to add
    sentry performance monitoring to function.
    Use without arguments to use function name,
    or optionally add custom name seen in sentry
    '''
    def _monitor_performance(f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            op_name = op or f.__name__
            with SentryTransaction(op=op_name):
                return f(*args, **kwargs)
        return wrapper
    return _monitor_performance
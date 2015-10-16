import logging
from flask import Flask, jsonify, redirect
from flask_swagger import swagger
import os

from datalake_api.v0 import v0
from datalake_api import settings


LOGGER = logging.getLogger(__name__)


app = Flask(__name__)
app.config.from_object(settings)
if 'DATALAKE_API_CONFIG' in os.environ:
    app.config.from_envvar('DATALAKE_API_CONFIG')
app.register_blueprint(v0)


level = os.environ.get('DATALAKE_API_LOG_LEVEL')
if level is not None and not app.debug:
    logging.basicConfig(level=level)


sentry_dsn = os.environ.get('SENTRY_DSN')
if sentry_dsn is not None:
    from raven.handlers.logging import SentryHandler
    handler = SentryHandler(sentry_dsn)
    app.logger.addHandler(handler)


@app.route('/')
def index():
    return redirect("/docs/", code=302)


@app.route("/docs/")
def docs():
    return redirect("/static/index.html", code=302)


@app.route("/spec/")
def spec():
    swag = swagger(app)
    swag['info']['version'] = "0"
    swag['info']['title'] = "Datalake API"
    swag['info']['description'] = 'Query files in the datalake archive'
    return jsonify(swag)


@app.route('/health/')
def health():
    return jsonify({})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

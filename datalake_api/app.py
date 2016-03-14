# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

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


level = app.config.get('DATALAKE_API_LOG_LEVEL')
if level is not None and not app.debug:
    logging.basicConfig(level=level)


sentry_dsn = app.config.get('SENTRY_DSN')
if sentry_dsn is not None:
    from raven.contrib.flask import Sentry
    sentry = Sentry(app, dsn=sentry_dsn)


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

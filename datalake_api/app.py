import logging
from flask import Flask, jsonify, redirect
from flask_swagger import swagger

from datalake_api.v0 import v0
from datalake_api import settings


LOGGER = logging.getLogger(__name__)


app = Flask(__name__)
app.config.from_object(settings)
app.register_blueprint(v0)


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

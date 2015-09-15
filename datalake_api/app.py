import logging
from flask import Flask

from datalake_api.v0 import v0
from datalake_api import settings


LOGGER = logging.getLogger(__name__)


app = Flask(__name__)
app.config.from_object(settings)
app.register_blueprint(v0)


@app.route('/')
def index():
    return 'hi'


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

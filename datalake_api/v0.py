import flask
import simplejson as json
import logging

from flask import jsonify, Response


v0 = flask.Blueprint('v0', __name__, url_prefix='/v0')


@v0.route('/archive/')
def archive_get():
    """
    Archive status

    Get the archive status.
    ---
    tags:
      - archive
    responses:
      200:
        description: success
    """
    return Response(json.dumps({}), content_type='application/json')

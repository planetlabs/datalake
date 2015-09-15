import flask
import simplejson as json
import logging

from flask import jsonify, Response


v0 = flask.Blueprint('v0', __name__, url_prefix='/v0')


@v0.route('/archive/')
def archive_get():
    params = flask.request.args
    response = params
    return Response(json.dumps(response), content_type='application/json')

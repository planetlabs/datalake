import flask
from flask import request, jsonify, Response
import simplejson as json
import logging
from copy import copy


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


@v0.errorhandler(400)
def handle_bad_request(err):
    return jsonify({'message': err.response, 'code': err.description}), 400


def _convert_param_to_ms(params, key):
    if key not in params:
        return
    try:
        params[key] = int(params[key])
    except ValueError:
        msg = key + ' must be milliseconds since the epoch.'
        flask.abort(400, 'InvalidTime', msg)


def _validate_files_params(params):
    if len(params) == 0:
        flask.abort(400, 'NoArgs', 'Please provide minimal query arguments')
    if 'what' not in params:
        flask.abort(400, 'NoWhat', 'You must provide the `what` paramater')
    if 'work_id' not in params and 'start' not in params and \
       'end' not in params:
        msg = 'You must provide either work_id or start/end'
        flask.abort(400, 'NoWorkInterval', msg)
    if 'work_id' in params and ('start' in params or 'end' in params):
        msg = 'You must provide only work_id or start/end. Not both.'
        flask.abort(400, 'InvalidWorkInterval', msg)
    if ('start' in params and 'end' not in params) or \
       ('end' in params and 'start' not in params):
        msg = 'start and end must always be provided together.'
        flask.abort(400, 'InvalidWorkInterval', msg)
    validated = {k: v for k, v in params.iteritems()}
    _convert_param_to_ms(validated, 'start')
    _convert_param_to_ms(validated, 'end')
    if 'start' in validated and 'end' in validated:
        if validated['start'] > validated['end']:
            msg = 'start must be before end'
            flask.abort(400, 'InvalidWorkInterval', msg)
    return validated


@v0.route('/archive/files/')
def files_get():
    '''List files

    Retrieve metadata for files subject to query parameters.

    You must always specify the `what` parameter.

    You must either specify work_id or start/end interval of the files in which
    you are interested.

    If you specify start you must also specify end.

    Returns metadata for at most 100 files in the list. If more files are
    available, the `next` property in the response will be a url that may be
    used to retrieve the next page of files.

    Note that no single page will contain duplicate files. However, under some
    circumstances, requests specifying a start and end time (as opposed to a
    work_id) may return duplicate records in subsequent pages. So applications
    that expect to retrieve multiple pages of results should tolerate
    duplicates. Alternatively, such applications could query for a narrower
    time interval.

    ---
    tags:
      - files
    parameters:
        - in: query
          name: what
          description:
              Only return files from here. May be a comma-separated list of
              many whats.
          type: string
          required: true
        - in: query
          name: where
          description:
              Only return files from here. May be a comma-separated list of
              many wheres.
          type: string
        - in: query
          name: work_id
          description:
              Only return files with this work_id. May be a comma-separated
              list of many work_ids.
          type: string
        - in: query
          name: start
          description:
              Only return files with data after this start time in ms since
              the epoch.
          type: integer
        - in: query
          name: end
          description:
              Only return files with data before this end time in ms since
              the epoch.
          type: integer
    responses:
      200:
        description: success
        schema:
          id: DatalakeMetadataList
          required:
              - metadata
              - next
          properties:
              metadata:
                  type: array
                  description: the list of metadata records matching the query.
                               May be an empty list
                  items:
                      schema:
                        id: DatalakeMetadata
                        required:
                            - version
                            - where
                            - start
                            - end
                            - work_id
                            - where
                            - data-version
                            - id
                            - hash
                        properties:
                            version:
                                type: integer
                                description: the version of the metadata record
                            where:
                                type: string
                                description: where the file came from
                            start:
                                type: integer
                                description: the start time of the file in ms
                                             since the epoch
                            end:
                                type: integer
                                description: the end time of the file in ms
                                             since the epoch. This may be
                                             null if the file is associated
                                             with an instant
                            work_id:
                                type: string
                                desription: the work_id associated with the
                                            file. This may be null.
                            where:
                                type: string
                                description: the location or server that
                                             generated the file
                            what:
                                type: string
                                description: the process or program that
                                             generated the file
                            data-version:
                                type: string
                                description: the version of the data format in
                                             the file
                            id:
                                type: string
                                description: the unique id of the file in the
                                             datalake
                            hash:
                                type: string
                                description: 16-byte blake2 hash of the file
                                             content

              next:
                  type: string
                  description: url to get the next results. Will be null if
                               there are no more results

      400:
        description: bad request
        schema:
          id: DatalakeAPIError
          required:
              - code
              - message
          properties:
              code:
                  type: string
                  description: code associated with this error
              message:
                  type: string
                  description: human-readable message indicating why the
                               request failed
    '''
    params = flask.request.args
    params = _validate_files_params(params)
    return Response(json.dumps({}), content_type='application/json')

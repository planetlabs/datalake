import flask
from flask import request, jsonify, Response, url_for
import simplejson as json
import logging
from flask import current_app as app
import boto3
from querier import ArchiveQuerier, Cursor, InvalidCursor


v0 = flask.Blueprint('v0', __name__, url_prefix='/v0')


dynamodb = None
def get_dynamodb():
    if not hasattr(app, 'dynamodb'):
        kwargs = dict(
            endpoint_url=app.config.get('DYNAMODB_ENDPOINT'),
            region_name=app.config.get('AWS_REGION'),
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY'),
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID')
        )
        app.dynamodb = boto3.resource('dynamodb', **kwargs)
    return app.dynamodb


def get_archive_querier():
    if not hasattr(app, 'archive_querier'):
        table_name = app.config.get('DYNAMODB_TABLE')
        app.archive_querier = ArchiveQuerier(table_name,
                                             dynamodb=get_dynamodb())
    return app.archive_querier


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
    validated = _copy_immutable_dict(params)
    _convert_param_to_ms(validated, 'start')
    _convert_param_to_ms(validated, 'end')
    if 'start' in validated and 'end' in validated:
        if validated['start'] > validated['end']:
            msg = 'start must be before end'
            flask.abort(400, 'InvalidWorkInterval', msg)
    _validate_cursor(validated)
    return validated


def _validate_cursor(params):
    try:
        params['cursor'] = _get_cursor(params)
    except InvalidCursor as e:
        flask.abort(400, 'InvalidCursor', e.message)


def _get_cursor(params):
    c = params.get('cursor')
    if c is None:
        return None
    return Cursor.from_serialized(c)


def _copy_immutable_dict(d):
    return {k: v for k, v in d.iteritems()}


@v0.route('/archive/files/')
def files_get():
    '''List files

    Retrieve metadata for files subject to query parameters.

    You must always specify the `what` parameter.

    You must either specify work_id or start/end interval of the files in which
    you are interested.

    If you specify start you must also specify end.

    Returns metadata for at most 100 files. If more files are available, the
    `next` property in the response will be a url that may be used to retrieve
    the next page of files.

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
              Only return files from here.
          type: string
          required: true
        - in: query
          name: where
          description:
              Only return files from here.
          type: string
        - in: query
          name: work_id
          description:
              Only return files with this work_id.
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

    aq = get_archive_querier()

    response = {}
    work_id = params.get('work_id')
    if work_id is not None:
        results = aq.query_by_work_id(work_id,
                                      params.get('what'),
                                      where=params.get('where'),
                                      cursor=params.get('cursor'))
    else:
        # we are guaranteed by the validate routine that this is a start/end
        # time-based query.
        results = aq.query_by_time(params['start'],
                                   params['end'],
                                   params['what'],
                                   where=params.get('where'),
                                   cursor=params.get('cursor'))

    response = {
        'metadata': results,
        'next': _get_next_url(flask.request, results),
    }
    return Response(json.dumps(response), content_type='application/json')


def _get_next_url(request, results):
    if results.cursor is None:
        return None
    return _get_url_with_cursor(request, results.cursor)


def _get_url_with_cursor(request, cursor):
    args = _copy_immutable_dict(request.args)
    args['cursor'] = cursor.serialized
    return url_for(request.endpoint, _external=True, **args)

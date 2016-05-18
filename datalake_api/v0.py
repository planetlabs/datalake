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

import flask
from flask import jsonify, Response, url_for
import simplejson as json
from flask import current_app as app
import boto3
from querier import ArchiveQuerier, Cursor, InvalidCursor, MAX_LOOKBACK_DAYS
from fetcher import ArchiveFileFetcher
from datalake_common.errors import NoSuchDatalakeFile


v0 = flask.Blueprint('v0', __name__, url_prefix='/v0')


def _get_aws_kwargs():
    kwargs = dict(
        region_name=app.config.get('AWS_REGION'),
    )
    for k in ['AWS_SECRET_ACCESS_KEY', 'AWS_ACCESS_KEY_ID']:
        # these guys must be fully absent from the kwargs; None will not
        # do.
        if app.config.get(k) is not None:
            kwargs[k.lower()] = app.config[k]
    return kwargs


def get_dynamodb():
    if not hasattr(app, 'dynamodb'):
        kwargs = _get_aws_kwargs()
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
    """Archive status

    Get the archive details.
    ---
    tags:
      - archive
    responses:
      200:
        description: success
        schema:
          id: DatalakeMetadataList
          required:
              - storage_url
          properties:
              storage_url:
                  type: string
                  description: base url where clients should push files.
    """
    response = dict(
        storage_url=app.config.get('DATALAKE_STORAGE_URL')
    )
    return jsonify(response)


@v0.errorhandler(400)
@v0.errorhandler(404)
def handle_4xx_status(err):
    body = {'message': err.response, 'code': err.description}
    return jsonify(body), err.code


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
          id: DatalakeRecordList
          required:
              - records
              - next
          properties:
              records:
                  type: array
                  description: the list of metadata records matching the query.
                               May be an empty list
                  items:
                      schema:
                        id: DatalakeRecord
                        required:
                          - url
                          - metadata
                        properties:
                          url:
                            type: string
                            description: s3 url where the file may be retrieved
                          http_url:
                            type: string
                            description: http url where the file contents
                          metadata:
                            schema:
                              id: DatalakeMetadata
                              required:
                                - version
                                - where
                                - start
                                - end
                                - path
                                - work_id
                                - where
                                - id
                                - hash
                              properties:
                                version:
                                  type: integer
                                  description: the version of the metadata
                                               record
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
                                path:
                                  type: string
                                  description: the path of the original file.
                                work_id:
                                  type: string
                                  description: the work_id associated with the
                                               file. This may be null.
                                where:
                                  type: string
                                  description: the location or server that
                                               generated the file
                                what:
                                  type: string
                                  description: the process or program that
                                               generated the file
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

    [r.update(http_url=_get_canonical_http_url(r)) for r in results]
    response = {
        'records': results,
        'next': _get_next_url(flask.request, results),
    }
    return Response(json.dumps(response), content_type='application/json')


def _get_canonical_http_url(record):
    return url_for('v0.file_get_contents', file_id=record['metadata']['id'])


def _get_next_url(request, results):
    if results.cursor is None:
        return None
    return _get_url_with_cursor(request, results.cursor)


def _get_url_with_cursor(request, cursor):
    args = _copy_immutable_dict(request.args)
    args['cursor'] = cursor.serialized
    return url_for(request.endpoint, _external=True, **args)


def get_s3_bucket():
    if not hasattr(app, 's3_bucket'):
        kwargs = _get_aws_kwargs()
        s3 = boto3.resource('s3', **kwargs)
        bucket_url = app.config.get('DATALAKE_STORAGE_URL')
        bucket_name = bucket_url.rstrip('/').split('/')[-1]
        app.s3_bucket = s3.Bucket(bucket_name)
    return app.s3_bucket


def get_archive_fetcher():
    if not hasattr(app, 'archive_fetcher'):
        app.archive_fetcher = ArchiveFileFetcher(get_s3_bucket())
    return app.archive_fetcher


def _get_file(file_id):
    try:
        aff = get_archive_fetcher()
        return aff.get_file(file_id)
    except NoSuchDatalakeFile as e:
        flask.abort(404, 'NoSuchFile', e.message)


def _get_headers_for_file(f):
    headers = {}
    if f.content_type is None:
        headers['Content-Type'] = 'text/plain'
    else:
        headers['Content-Type'] = f.content_type
    if f.content_encoding is not None:
        headers['Content-Encoding'] = f.content_encoding
    return headers


def _get_latest(what, where):
    aq = get_archive_querier()
    f = aq.query_latest(what, where)
    if f is not None:
        return f

    m = 'No "{}" files found in last {} days from "{}"'
    m = m.format(what, MAX_LOOKBACK_DAYS, where)
    flask.abort(404, 'NoSuchFile', m)


@v0.route('/archive/files/<file_id>/data')
def file_get_contents(file_id):
    '''Retrieve a file

    Retrieve a file's contents.
    ---
    tags:
      - file contents
    parameters:
        - in: path
          name: file_id
          description:
              The id of the file to retrieve
          type: string
          required: true
    responses:
      200:
        description: success
        schema:
          type: file
      404:
        description: no such file
        schema:
          id: DatalakeAPIError
    '''
    f = _get_file(file_id)
    headers = _get_headers_for_file(f)
    return f.read(), 200, headers


@v0.route('/archive/files/<file_id>/metadata')
def file_get_metadata(file_id):
    '''Retrieve metadata for a file

    Retrieve a file's metadata.
    ---
    tags:
      - file contents
    parameters:
        - in: path
          name: file_id
          description:
              The id of the file whose metadata to retrieve
          type: string
          required: true
    responses:
      200:
        description: success
        schema:
          id: DatalakeMetadata
      404:
        description: no such file
        schema:
          id: DatalakeAPIError
    '''
    f = _get_file(file_id)
    return Response(json.dumps(f.metadata), content_type='application/json')


@v0.route('/archive/latest/<what>/<where>')
def latest_get(what, where):
    '''Retrieve the latest file for a give what and where

    Retrieve latest file. Note that the current implementation of latest only
    tracks the last 14 days of files. If you expect files older than this, you
    must retrieve them using the files endpoint.

    ---
    tags:
      - latest
    parameters:
        - in: path
          name: what
          description:
              The process or program of interest
          type: string
          required: true
        - in: path
          name: where
          description:
              The location of interest (e.g., server or location)
          type: string
          required: true
    responses:
      200:
        description: success
        schema:
          id: DatalakeRecord
      404:
        description: no latest file found for the given what or where in the
                     last 14 days.
        schema:
          id: DatalakeAPIError

    '''
    f = _get_latest(what, where)
    f.update(http_url=_get_canonical_http_url(f))
    return Response(json.dumps(f), content_type='application/json')


@v0.route('/archive/latest/<what>/<where>/data')
def latest_get_contents(what, where):
    '''Retrieve the latest file data for a given what and where

    Note that the current implementation of latest only tracks the last 14 days
    of files. If you expect files older than this, you must retrieve them using
    the files endpoint.
    ---
    tags:
      - latest
    parameters:
        - in: path
          name: what
          description:
              The process or program of interest
          type: string
          required: true
        - in: path
          name: where
          description:
              The location of interest (e.g., server or location)
          type: string
          required: true
    responses:
      200:
        description: success
        schema:
          type: file
      404:
        description: no latest file found for the given what or where in the
                     last 14 days.
        schema:
          id: DatalakeAPIError

    '''
    f = _get_latest(what, where)
    f = _get_file(f['metadata']['id'])
    headers = _get_headers_for_file(f)
    return f.read(), 200, headers

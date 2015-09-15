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


@v0.route('/archive/files/')
def files_get():
    """List files

    Retrieve metadata for files subject to query parameters.

    You must always specify the `what` parameter.

    You must either specify work_id or start/end interval of the files in which
    you are interested.

    If you specify start you must also specify end.

    Returns metadata for at most 100 files in the list. If more than 100 files
    are available, the next property in the response will be a url that may be
    used to retrieve the next batch of files.

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
    """
    return Response(json.dumps({}), content_type='application/json')

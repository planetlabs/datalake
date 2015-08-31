Introduction
============

A datalake is an archive that contains files and metadata records about those
files. datalake-common is a place for code shared by the handful of components
that form the datalake. Mostly, datalake-common is about defining and
validating the schemas that are shared between these components.

Datalake Metadata
=================

Files that are shipped to the datalake are accompanied by a JSON metadata
document. Here it is:

        {
            "version": 0,
            "start": 1426809920345,
            "end": 1426895999114,
            "work_id": null,
            "where": "webserver02",
            "what": "syslog",
            "data-version": "0",
            "id": "6309e115c2914d0f8622973422626954",
            "hash": "a3e75ee4f45f676422e038f2c116d000"
        }

version: This is the metadata version. It should be 0.

start: This is the time of the first event in the file in milliseconds since
the epoch. Alternatively, if the file is associated with an instant, this is
the only relevant time. It is required.

end: This is the time of the last event in the file in milliseconds since the
epoch. If it is not present, the file represents a snapshot of something like a
weekly report.

where: This is the location or server that generated the file. It is required
and must only contain lowercase alpha-numeric characters, - and _. It should be
concise. 'localhost' and 'vagrant' are bad names. Something like
'whirlyweb02-prod' is good.

what: This is the process or program that generated the file. It is required
and must only contain lowercase alpha-numeric characters, - and _. It must not
have trailing file extension (e.g., .log). The name should be concise to limit
the chances that it conflicts with other whats in the datalake. So names like
'job' or 'task' are bad. Names like 'balyhoo-source-audit' or
'rawfood-ingester' are good.

data-version: This is the data version. It must only contain alpha-numeric
characters, -, and _. But the format is otherwise up to the user. If the format
of the contents of the file changes, this version should change so that
consumers of the data can know to use a different parser. It is required.

id: An ID for the file assigned by the datalake. It is required.

hash: A 16-byte blake2 hash of the file content. This is calcluated and
assigned by the datalake. It is required.

work_id: This is an application-specific id that can be used later to retrieve
the file. It is required but may be null. In fact the datalake utilities will
generally default it to null if it is not set. It must not be the string
"null". It should be prepended with a domain-specific prefix to prevent
conflicts with other work id spaces. It must only contain lowercase
alpha-numeric characters, -, and _.

Developer Setup
===============

        mkvirtualenv datalake # Or however you like to manage virtualenvs
        pip install -e .[test]
        py.test

Introduction
============

NOTE: Much of this README is an aspirational.

A datalake is an archive that contains files and metadata records about those
files. This datalake project is a python library and command-line tool for
managing a datalake. You can use it to push files to the datalake, list the
files available in the datalake, and retrieve files from the datalake.

Where do the files get archived? In an s3 bucket that you configure. And
perhaps eventually in other kinds of storage.

Why would I use this? Because you just want to get all of the files into one
place with nice uniform metadata so you can know what is what. Then you can
pull the files onto your hardrive for your grepping and awking pleasure. Or
perhaps you can feed them to a compute cluster of some sort for mapping and
reducing. Or maybe you don't want to set up and maintain a bunch of log
ingestion infrastructure, or you don't trust that log ingestion infrastructure
to be your source of truth. Or maybe you just get that warm fuzzy feeling when
things are archived somewhere.

Isn't this a solved problem? Kinda. s3cmd + logrotate can get you pretty
close. The thing that datalake adds is a query capability.

Configuration
=============

datalake needs a bit of configuration. Every configuration variable can either
be set in /etc/datalake.conf, set as an environment variable, or passed in as
an argument. For documentation on the configuration variables, invoke `datalake
-h`.

Usage
=====

datalake has a python API and a command-line client. What you can do with one,
you can do with the other. Here's how it works:

Push a log file:

        datalake push --start 2015-03-20T00:05:32.345Z
            --end 2015-03-20T23:59.114Z \
            --where webserver01 --what nginx /path/to/nginx.log \
            --data-version 0

Push a log file with a specific work-id:

        datalake push --start 2015-03-20T00:00:05:32.345Z \
            --end 2015-03-20T00:00:34.114Z \
            --what blappo-etl --where backend01 --data-version 0 \
            --work-id blappo-14321359

The work-id is convenient for tracking processing jobs or other entities that
may pass through many log-generating machines as they proceed through life. It
must be unique within the datalake. So usually some kind of domain-specific
prefix is recommended here.

List the syslog and foobar files available from webserver01 since the specified
start date.

        datalake list --where webserver01 --start 2015-03-20 --end `date -u` \
            --what syslog,foobar

Fetch the blappo gather, etl, and cleanup log files with work id
blappo-14321359:

        datalake fetch --what gather,etl,cleanup --work-id blappo-14321359

Metadata
========

How is all of this magic achieved? Mostly just by keeping some metadata around
with each file. In JSON, the metadata looks something like this:

        {
            "version": "0",
            "start": 1426809920345,
            "end": 1426895999114,
            "work_id": null,
            "where": "webserver02",
            "what": "syslog",
            "data-version": "0",
            "id": "6309e115c2914d0f8622973422626954",
            "hash": "a3e75ee4f45f676422e038f2c116d000"
        }

version: This is the metadata version. It should be "0".

start: This is the time of the first event in the file in milliseconds since
the epoch. Alternatively, if the file is associated with an instant, this is
the only relevant time. It is required.

end: This is the time of the last event in the file in milliseconds since the
epoch. If it is not present, the file represents a snapshot of something like a
weekly report.

where: This is the location or server that generated the file. It is required
and must only contain alpha-numeric characters, - and _. It should be
concise. 'localhost' and 'vagrant' are bad names. Something like
'whirlyweb02-prod' is good.

what: This is the process or program that generated the file. It is required
and must only contain alpha-numeric characters, - and _. It must not have
trailing file extension (e.g., .log). The name should be concise to limit the
chances that it conflicts with other whats in the datalake. So names like 'job'
or 'task' are bad. Names like 'balyhoo-source-audit' or 'rawfood-ingester' are
good.

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
conflicts with other work id spaces.

Developer Setup
===============

        mkvirtualenv datalake # Or however you like to manage virtualenvs
        pip install -e .[test]
        py.test

Please note that you must periodically re-run the pip install to ensure that
the command-line client is installed properly or some tests may not pass.

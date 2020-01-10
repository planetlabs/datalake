Introduction
============

[![Build Status](https://travis-ci.org/planetlabs/datalake.svg)](https://travis-ci.org/planetlabs/datalake)

A datalake is an archive that contains files and metadata records about those
files. The datalake project consists of a number of pieces:

- The ingester that listens for new files pushed to the datalake and ingests
  their metadata so it can be searched.

- The api to query over the files in the datalake.

- The client, which is a python and command-line interface to the datalake. You
  can use it to push files to the datalake, list the files available in the
  datalake, and retrieve files from the datalake.

To use this client, you (or somebody on your behalf) must be operating an
instance of the datalake-ingester and the datalake-api. You will need some
configuration information from them.

Why would I use this? Because you just want to get all of the files into one
place with nice uniform metadata so you can know what is what. Then you can
pull the files onto your hardrive for your grepping and awking pleasure. Or
perhaps you can feed them to a compute cluster of some sort for mapping and
reducing. Or maybe you don't want to set up and maintain a bunch of log
ingestion infrastructure, or you don't trust that log ingestion infrastructure
to be your source of truth. Or maybe you just get that warm fuzzy feeling when
things are archived somewhere.

Client Usage
============

Install
-------

        pip install datalake

If you plan to use the queuing feature, you must install some extra
dependencies:

        apt-get install libffi-dev # or equivalent
        pip install datalake[queuable]

Configure
---------

datalake needs a bit of configuration. Every configuration variable can either
be set in /etc/datalake.conf, set as an environment variable, or passed in as
an argument. For documentation on the configuration variables, invoke `datalake
--help`.

Usage
-----

datalake has a python API and a command-line client. What you can do with one,
you can do with the other. Here's how it works:

Push a log file:

        datalake push --start 2015-03-20T00:05:32.345Z
            --end 2015-03-20T23:59.114Z \
            --where webserver01 --what nginx /path/to/nginx.log

Push a log file with a specific work-id:

        datalake push --start 2015-03-20T00:00:05:32.345Z \
            --end 2015-03-20T00:00:34.114Z \
            --what blappo-etl --where backend01 \
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

Developer Setup
===============

        make docker test

Datalake Metadata
=================

Files that are shipped to the datalake are accompanied by a JSON metadata
document. Here it is:

        {
            "version": 0,
            "start": 1426809920345,
            "end": 1426895999114,
            "path": "/var/log/syslog.1"
            "work_id": null,
            "where": "webserver02",
            "what": "syslog",
            "id": "6309e115c2914d0f8622973422626954",
            "hash": "a3e75ee4f45f676422e038f2c116d000"
        }

version: This is the metadata version. It should be 0.

start: This is the time of the first event in the file in milliseconds since
the epoch. Alternatively, if the file is associated with an instant, this is
the only relevant time. It is required.

end: This is the time of the last event in the file in milliseconds since the
epoch. If the key is not present or if the value is `None`, the file represents a
snapshot of something like a weekly report where only one date (`start`) is
relevant.

path: The absolute path to the file in the originating filesystem.

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

id: An ID for the file assigned by the datalake. It is required.

hash: A 16-byte blake2 hash of the file content. This is calcluated and
assigned by the datalake. It is required.

work_id: This is an application-specific id that can be used later to retrieve
the file. It is required but may be null. In fact the datalake utilities will
generally default it to null if it is not set. It must not be the string
"null". It should be prepended with a domain-specific prefix to prevent
conflicts with other work id spaces. It must only contain lowercase
alpha-numeric characters, -, and _.

Index Design
============

In practice, metadata is stored in DynamoDB, which has strict but simple rules
about defining and querying indexes. We wish to support a few simple queries
over our metadata:

1. give me all of the WHATs for a given WHERE from t=START to t=END
2. give me all of the WHATs from t=START to t=END
3. give me all of the WHATs for a given WHERE with a given WORK_ID
4. give me all of the WHATs with a given WORK_ID

To achieve this using DynamoDB, we adopt the notion of "time buckets," each of
which is one day long. So a file whose data spans the period of today from
1:00-2:00 would have a single record in today's time bucket. A file whose data
spans the period from yesterday at noon to today at noon has two records: one
in yesterday's bucket and one in today's bucket. And so when a user queries
over a time period, we simply calculate the buckets that the time period spans,
then look in each bucket for relevant files.

But doesn't that mean we have to sometimes write multiple records per file?
Yes. What if a file spans 100 days? Do we really want to put a record in each
of 100 buckets? Well, this would be a pretty uncommon case for the uses that we
are envisioning. In practice, such files should be broken up into smaller files
and uploaded more frequently. What if a user queries for 100 days worth of
data?  Well, we examine a bunch of buckets and it takes a while. Users that are
not prepared to wait this long should make smaller requests.

To enable these queries, we have two hash-and-range indexes. They have the
following HASHKEY RANGEKEY format:

        TIME_BUCKET:WHAT WHERE:ID

        WORK_ID:WHAT WHERE:ID

The first index is to support query types 1 and 2. By using TIME_BUCKET:WHAT as
the hash key we prevent "hot" hash keys by distributing writes and queries
across WHATs. So while all the records for a day will be written to the same
TIME_BUCKET, and while users are much more likely to query recent things from
the last few TIME_BUCKETs, we spread the load across a diversity of WHATs. The
WHERE:ID range key can be used to retrieve a subset of WHEREs if
necessary. Finally, we append the file ID to ensure that the key is unique as
required by DynamoDB.

The second index supports query types 3 and 4 and follows a pattern similar to
the first. However, it should be noted that the WORK_ID is optional metadata,
but required for indexing purposes. To work around this without introducing a
hot hash key in the second index, the ingester generates a random WORK_ID with
the reserved prefix "null".

Datalake Record Format
======================

The datalake client specifies metadata that is recorded when a file is pushed
to the datalake. We need to store some administrative fields to get our queries
to work with the dynamodb indexes. These records have the following format:

        {
            "version": 0,
            "url": "s3://datalake/d-nebraska/nginx/1437375600000/91dd2525a5924c6c972e3d67fee8cda9-nginx-523.txt",
            "time_index_key": "16636:nginx",
            "work_id_index_key": "nullc177bfc032c548ba9e056c8e8672dba8:nginx",
            "range_key": "nebraska:91dd2525a5924c6c972e3d67fee8cda9",
            "create_time": 1426896791333,
            "size": 7892341,
            "metadata": { ... },
        }

version: the version of the datalake record format. What we describe here is
version 0.

url: the url of the resource to which the datalake record pertains.

time_index_key: the hash key for the index used for time-based queries. It is
formed by joining the "time bucket" number and the "what" from the metadata.

work_id_index_key: the hash key for the index used for work_id-based
queries. It is formed by joining the work_id and the "what" from the
metadata. Note that if the work_id is null, a random work_id will be generated
to prevent ingestion failures and hot hash keys. Of course in this case
retrieving by work_id is not meaninful or possible.

range_key: the range key used by the time-based and work_id-based indexes. It
is formed by joining the "where" and the "id" from the metadata.

create_time: the creation time of the file in the datalake

size: the size of the file in bytes

Ingester
========

The datalake-ingester ingests datalake metadata records into a database so that
they may be queried by other datalake components.

The ingester looks something like this:

                                               +----------+     +---------+
           +-------+    +-----------------+    |          |---->| storage |
        -->| queue |--->| s3_notification |--->| ingester |     +---------+
           +-------+    +-----------------+    |          |--+
                                               +----------+  |  +----------+
                                                             +->| reporter |
                                                                +----------+


A queue receives notice that an event has occured in the datalake's s3
bucket. An s3_notification object translates the event from the queue's format
to the datalake record format (see above). Next the ingester updates the
storage (i.e., dynamodb) and reports the ingestion status to the reporter
(i.e., SNS).

Datalake Ingester Report Format
===============================

The datalake ingester emits a Datalake Ingester Report for each file that it
ingests. The report has the following format:

        {
            "version": 0,
            "status": "success",
            "start": 1437375854967,
            "duration": 0.738383,
			"records": [
                {
                    "url": "s3://datalake/d-nebraska/nginx/1437375600000/91dd2525a5924c6c972e3d67fee8cda9-nginx-523.txt",
                    "metadata": { ... }
                }
            ]
        }

version: the version of the datalake ingester report format. What we describe
here is version 0.

status: Either "success", "warning", or "error" depending on how successful
ingestion was. If status is not "success" expect "message" to be set with a
human-readable explanation.

start: ms since the epoch when the ingestion started.

duration: time in seconds that it took to ingest the record.

records: a list of records that were ingested. Note that this is typically a
list with one element. However, some underlying protocols (e.g., s3
notifications) may carry information about multiple records. Under these
circumstances multiple records may appear.

API
===

The datalake-api offers and HTTP interface to the datalake.

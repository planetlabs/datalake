[![Build Status](https://travis-ci.org/planetlabs/datalake-common.svg?branch=master)](https://travis-ci.org/planetlabs/datalake-common)

Introduction
============

A datalake is an archive that contains files and metadata records about those
files. datalake-common is a place for code and specification shared by the
handful of components that form the datalake. Mostly, datalake-common is about
defining and validating the schemas that are shared between these components.

Installation
============

For basic metadata handling, just:

        pip install datalake-common

If you require s3-based features, be sure to ask for them:

        pip install datalake-common[s3]

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

Developer Setup
===============

        mkvirtualenv datalake # Or however you like to manage virtualenvs
        pip install -e .[test]
        py.test

Do `pip install -e .[test,s3,test-s3]` instead to work on the s3-enabled
features.

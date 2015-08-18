Introduction
============

The datalake-backend ingests datalake metadata records into a database so that
they may be queried by the datalake client.

Architecture Notes
==================

The backend looks something like this:

                                  +----------+     +---------+
   +-------+    +------------+    |          |---->| storage |
-->| queue |--->| translator |--->| ingester |     +---------+
   +-------+    +------------+    |          |--+
                                  +----------+  |  +----------+
                                                +->| reporter |
                                                   +----------+


A queue receives notice that a new file has been uploaded to the datalake. A
translator translates the event from the queue-specific format to the datalake
record format (see below). Next the ingester writes the ingestion record to the
storage and reports the ingestion status to the reporter.

In practice, the queue is an SQS queue the receives S3 notifications. The
translator translates from the S3 event format to our own datalake record
format. The storage is a DynamoDB table. The reporter is an SNS topic.

Index Design
============

DynamoDB has strict but simple rules about defining and querying indexes. We
wish to support a few simple queries over our metadata:

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
following <HASH-KEY>:<RANGE-KEY> format:

<TIME_BUCKET>-<WHAT>:<WHERE>-<ID>
<WORK_ID>-<WHAT>:<WHERE>-<ID>

The first index is to support query types 1 and 2. By using
<TIME_BUCKET>-<WHAT> as the hash key we prevent "hot" hash keys by distributing
writes and queries across WHATs. So while all the records for a day will be
written to the same TIME_BUCKET, and while users are much more likely to query
recent things from the last few TIME_BUCKETs, we spread the load across a
diversity of WHATs. The <WHERE>-<ID> range key can be used to retrieve a subset
of WHEREs if necessary. Finally, we append the file <ID> to ensure that the key
is unique as required by DynamoDB.

The second index supports query types 3 and 4 and follows a pattern similar to
the first. However, it should be noted that the WORK_ID is optional metadata,
but required for indexing purposes. To work around this without introducing a
hot hash key in the second index, the ingester generates a random WORK_ID with
the reserved prefix "null".

Datalake Record Format
======================

The datalake client specifies metadata that is recorded when a file is pushed
to the datalake. We need to store some administrative fields to get our queries
to work with dynamodb. These records have the following format:

{
    "version": 0,
    "url": "s3://datalake/d-nebraska/nginx/1437375600000/91dd2525a5924c6c972e3d67fee8cda9-nginx-523.txt",
    "time_index_key": "16636-nginx",
    "work_id_index_key": "nullc177bfc032c548ba9e056c8e8672dba8-nginx",
	"range_key": "nebraska-91dd2525a5924c6c972e3d67fee8cda9",
    "metadata": { ... },
}

version: the version of the datalake record format. What we describe here is
version 0.

url: the url of the resource to which the datalake record pertains.

time_index_key: the hash key for the index used for time-based queries. It is
formed by joining the "time bucket" number and the "what" from the metadata.

work_id_index_hash: the hash key for the index used for work_id-based
queries. It is formed by joining the work_id and the "what" from the
metadata. Note that if the work_id is null, a random work_id will be generated
to prevent ingestion failures and hot hash keys.

range_key: the range key used by the time-based and work_id-based indexes. It
is formed by joining the "where" and the "id" from the metadata.

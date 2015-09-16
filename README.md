Introduction
============

[![Build Status](https://travis-ci.org/planetlabs/datalake-ingester.svg)](https://travis-ci.org/planetlabs/datalake-ingester)

The datalake-ingester ingests datalake metadata records into a database so that
they may be queried by other datalake components.

Architecture Notes
==================

The ingester looks something like this:

                                          +----------+     +---------+
           +-------+    +------------+    |          |---->| storage |
        -->| queue |--->| translator |--->| ingester |     +---------+
           +-------+    +------------+    |          |--+
                                          +----------+  |  +----------+
                                                        +->| reporter |
                                                           +----------+


A queue receives notice that a new file has been uploaded to the datalake. A
translator translates the event from the queue-specific format to the datalake
record format (see
[datalake-common](https://github.com/planetlabs/datalake-common)). Next the
ingester writes the ingestion record to the storage and reports the ingestion
status to the reporter.

In practice, the queue is an SQS queue the receives S3 notifications. The
translator translates from the S3 event format to our own datalake record
format. The storage is a DynamoDB table. The reporter is an SNS topic. But the
abstractions in place should permit porting to other services.

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

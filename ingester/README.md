Introduction
============

[![Build Status](https://travis-ci.org/planetlabs/datalake-ingester.svg)](https://travis-ci.org/planetlabs/datalake-ingester)

The datalake-ingester ingests datalake metadata records into a database so that
they may be queried by other datalake components.

Architecture Notes
==================

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
to the datalake record format (see
[datalake-common](https://github.com/planetlabs/datalake-common)). Next the
ingester updates the storage (i.e., dynamodb) and reports the ingestion status
to the reporter (i.e., SNS).

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

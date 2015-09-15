Introduction
============

[![Build Status](https://travis-ci.org/planetlabs/datalake.svg)](https://travis-ci.org/planetlabs/datalake)

NOTE: Much of this README is an aspirational.

A datalake is an archive that contains files and
[metadata](https://github.com/planetlabs/datalake-common) records about those
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

Developer Setup
===============

        mkvirtualenv datalake # Or however you like to manage virtualenvs
        pip install -e .[test]
        py.test

Please note that you must periodically re-run the pip install to ensure that
the command-line client is installed properly or some tests may not pass.

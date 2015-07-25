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

        datalake push --start 2015-03-20T00:05.345Z
            --end 2015-03-20T23:59.114Z \
            --where webserver01 --what nginx /path/to/nginx.log \
            --data-version 0

Push a log file, specifying some extra detail:

        datalake push --start 2015-03-20T00:00:05.345Z \
            --end 2015-03-20T23:59:59.114Z \
            --what syslog --where webserver01 --data-version 0 \
            --tags app=MemeGenerator,magic=123 /path/to/my.log

List the syslog and foobar files available from webserver01 since the specified
start date.

        datalake list --where webserver01 --start 2015-03-20 --end `date -u` \
            --what syslog,foobar

Fetch the nginx log files from webserver01 and webserver02 to the current
directory:

        datalake fetch --where webserver01,webserver02 --start 2015-03-20 \
            --end `date -u` --what nginx

Metadata
========

How is all of this magic achieved? Mostly just by keeping some metadata around
with each file. In JSON, the metadata looks something like this:

        {
            "version": "0",
            "start": "2015-03-20T00:05:00.345Z",
            "end": "2015-03-20T23:59:59.114Z",
            "where": "webserver02",
            "what": "syslog",
            "data-version": "0",
            "tags": {
                "app": "MemeGenerator",
                "magic": 123
            }
        }

version: This is the metadata version. It should be "0".

start: This is the time of the first event in the file. Alternatively, if the
file is associated with an instant, this is the only relevant time. It is
required.

end: This is the time of the last event in the file. If it is not present, the
file represents a snapshot of something like a weekly report.

where: This is the location or server that generated the file. It is required.

what: This is the process or program that generated the file. It is required.

data-version: This is the data version. The format of the version is up to the
user. If the format of the contents of the file changes, this version should
change so that consumers of the data can know to use a different parser. It is
required.

tags: Arbitrary extra tags specified when the file was archived.

Developer Notes
===============

To fetch the dependencies and run the tests:

        pip install -r requirements.txt -r test-requirements.txt
        nosetests

To package up datalake and publish it, get yourself added as an owner or
maintainer for the project in pypi, prepare a [.pypirc
file](https://docs.python.org/3.1/distutils/packageindex.html#pypirc), and
upload the package:

        python setup.py sdist upload

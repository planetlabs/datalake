Introduction
============

NOTE: This is an aspirational README.

atl stands for "all the logs" or "archive the log" if you like. It is a python
library and a command-line tool for managing an archive of log files. You can
use atl to push log files to the archive, list the log files available in the
archive, and retrieve log files from the archive.

Where do the logs get archived? In an s3 bucket that you configure.

Why would I use this? Because you just want to get all of the logs onto your
hardrive for your grepping and awking pleasure. Or maybe you don't want to set
up and maintain a bunch of log ingestion infrastructure. Or maybe you just get
that warm fuzzy feeling when things are archived somewhere.

Isn't this a solved problem? Kinda. s3cmd + logrotate can get you pretty
close. The thing that atl adds is a query capability.

Configuration
=============

atl needs a bit of configuration. Every configuration variable can either be
set in /etc/atl.conf, set as an environment variable, or passed in as an
argument. For documentation on the configuration variables, invoke `atl -h`.

Usage
=====

atl has a python API and a command-line client. What you can do with one, you
can do with the other. Here's how it works:

Push a log file:

        atl push --start 2015-03-20T00:05.345Z --end 2015-03-20T23:59.114Z \
            --where webserver01 --what nginx /path/to/nginx.log
            --data-version 0

Push a log file, specifying some extra detail:

        atl push --start 2015-03-20T00:00:05.345Z --end 2015-03-20T23:59:59.114Z \
            --what syslog --where webserver01 --data-version 0 \
            --tags app=MemeGenerator,magic=123 /path/to/my.log

List the syslog and foobar files available from webserver01 since the specified
start date.

        atl list --where webserver01 --start 2015-03-20 --end `date -u` \
                 --what syslog,foobar

Fetch the nginx log files from webserver01 and webserver02 to the current
directory:

        atl fetch --where webserver01,webserver02 --start 2015-03-20 \
                  --end `date -u` --what nginx

Metadata
========

How is all of this magic achieved? Mostly just by keeping some metadata around
with each file. Any metadata that is not provided will be guessed. In JSON, the
metadata looks something like this:

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

start: This is the time of the first event in the log. It is required.

end: This is the time of the last event in the log. It is required.

where: This is the location or server that generated the log file. It is
required.

what: This is the process or program that generated the log. It is required.

data-version: This is the data version. The format of the version is up to the
user. If the format of the contents of the log files changes, this version
should change so that consumers of the data can know to use a different
parser. It is required.

tags: Arbitrary extra tags specified to atl when the log was archived.

Developer Notes
===============

To fetch the dependencies and run the tests:

        pip install -r requirements.txt -r test-requirements.txt
        nosetests

To package up atl and publish it, get yourself added as an owner or maintainer
for the project in pypi, prepare a [.pypirc
file](https://docs.python.org/3.1/distutils/packageindex.html#pypirc), and
upload the package:

        python setup.py sdist upload

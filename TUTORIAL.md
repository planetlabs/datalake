First, you are going to need an instance of the datalake-ingester and
datalake-api up and running. Making this happen is beyond the scope of this
tutorial. So I'll just assume that you (or somebody who loves you) is operating
a datalake on your behalf. That somebody can tell you the DATALAKE_STORAGE_URL
and DATALAKE_HTTP_URL. For the purposes of this tutorial I'll assume that
they are:

        DATALAKE_STORAGE_URL=s3://my-datalake
        DATALAKE_HTTP_URL=https://my-datalake

First, you're going to want to install the datalake client:

        pip install datalake

Basic Commandline Usage
=======================

Prepare the environment. For this tutorial, we just set everything as
environment variables:

        export AWS_REGION=us-gov-west-1
        export DATALAKE_STORAGE_URL=s3://my-datalake
        export DATALAKE_HTTP_URL=https://my-datalake
        export AWS_ACCESS_KEY_ID=XXXXX
        export AWS_SECRET_ACCESS_KEY=YYYYY

There are a number of other ways that you can set your configuration. For
example, you can write /etc/datalake.env, which is the default configuration
file. If you would wrather write the file somewhere else, just pass it in to
datalake with the -c option.

Now create a hypothetical log file describing your work day on October 1:

        cat <<EOF > ${USER}.log
        2015-10-01T07:00:00 wake up.
        2015-10-01T09:00:00 eat breakfast. Mmm.
        2015-10-01T10:30:00 arrive at work.
        2015-10-01T18:00:00 happy hour.
        EOF
        > ${USER}.log

Note that the datalake does not inspect the content. So you could have just
written a bunch of random stuff in here. But the datalake is designed to hold
files that have a start and end time associated with them.

Push the file to the datalake:

        datalake push --start=2015-10-01 --end=2015-10-02 \
                 --where server123 --what ${USER} ${USER}.log


Now you can query for the file:

        curl "${DATALAKE_HTTP_URL}/v0/archive/files/?what=${USER}&start=1443657600000&end=1443657600001"

Expect some structured metadata about your file back, including a url. You can
fetch the file using the URL and [s3cmd](http://s3tools.org/s3cmd). Note:
improved integration with datalake client is forthcoming!

Metadata Automation
===================

You may want to automate pushing to the datalake. Like perhaps when log rotate
runs, you may want it to push the rotated log to the datalake. One problem that
you will face is gathering all of the required metadata. The datalake offers a
number of different features to help with this.

DATALAKE_DEFAULT_WHERE
----------------------

Often the datalake client runs on a single machine, and that machine's hostname
will always be the `--where` required by the metadata. If this is the case, you
can set the DATALAKE_DEFAULT_WHERE configuration variable and omit the --where
argument.

--start=crtime --end=now
------------------------

The start of a log file is often the time when it was created. The end of a log
file is often "now". Like the moment when log rotate is rotating a log file. To
exploit this fact, you can pass `--start=crtime` and `--end=now` to datalake.

Please note that for the crtime feature to work you must have the
[crtime](https://github.com/planetlabs/crtime) utility installed, the crtime
executable must be setuid root, and the file must be in a filesystem supported
by e2fsprogs.

translation expressions
-----------------------

Often relevant metadata fields are embedded in file names. You can extract
these using translation expressions, which look like this:

        <extraction_expression>~<format_expression>

The `<extraction_expression>` is a regular expression with at least one named
group. It is separated from the `<format_expression>` by the `~` character. The
`<format_expression>` is a template specifying the desired format. It may
contain references to the named groups enclosed in braces. For example, suppose
you have files with filenames that look like this:

        /var/log/jobs/job-1234.log

Let's say that we want to somehow pass `--work-id=job1234` to datalake. We can
achieve this with a translation expression that looks like this:

        --work-id='.*job-(?P<job_id>[0-9]+).log$~job{job_id}'

All metadata values may be specified using translation expressions. Further,
the translation expressions are applied to the absolute path of the argument
file even if the file was specified with a relative path.

The command `datalake translate` will apply a specified translation expression
to a specified string. This is useful for testing and developing translation
expressions. For example:

        datalake translate '.*job-(?P<job_id>[0-9]+).log$~job{job_id}' \
                           /var/log/jobs/job-1234.log

...will return:

        job1234

Note that translation expressions are only supported for --where, --what, and
--work-id.

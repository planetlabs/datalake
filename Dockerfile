FROM python:3.9-slim

MAINTAINER brian <brian@planet.com>

ENV LANG C.UTF-8
ENV	LC_ALL C.UTF-8


# To generate the pip requirements list
# import re
# import distutils.core
# s = [distutils.core.run_setup(f+"/setup.py") for f in ("ingester","api","client")]
# reqs = [[i for k,v in s1.extras_require.items() for i in v] + s1.install_requires for s1 in s]
# print(" ".join(set([re.split(r'[<>=]', req)[0] for i in reqs for req in i])))

RUN pip install memoized_property flask-swagger boto moto click flake8 sentry-sdk[flask] twine raven pip boto3 six python-dotenv pytest requests pytz freezegun wheel python-dateutil blinker pyinotify sentry-sdk Flask responses simplejson

RUN mkdir -p /opt/
COPY . /opt/

# Take care to install clients such that the source code can be mounted into
# the container and used for development. That is, the python paths and paths
# to console scripts Just Work (TM)
ENV PYTHONPATH=/opt/client:/opt/ingester:/opt/api
RUN for d in client ingester api; do \
    cd /opt/$d && \
    python setup.py develop -s /usr/local/bin \
        --egg-path ../../../../../opt/$d/ \
        --no-deps; \
    done

ARG VERSION=unspecified
ENV VERSION=$VERSION

COPY version.txt /

WORKDIR /opt
ENTRYPOINT ["/opt/docker_entry.sh"]

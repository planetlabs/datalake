FROM python:3.9-slim

MAINTAINER brian <brian@planet.com>

ENV LANG C.UTF-8
ENV	LC_ALL C.UTF-8

# TODO: keep requirements in one place
RUN pip install \
    blinker>=1.4 \
    boto>=2.38.0 \
    boto3>=1.1.3 \
    click>=5.1 \
    Flask>=0.10.1 \
    flask-swagger>=0.2.14 \
    memoized_property>=1.0.1 \
    python-dateutil>=2.4.2 \
    python-dotenv>=0.1.3 \
    pytz>=2015.4 \
    sentry-sdk[flask]>=0.19.5 \
    requests>=2.5 \
    simplejson>=3.3.1 \
    six>=1.10.0 \
    # test requirements
    'flake8>=2.5.0,<4.1' \
    'freezegun<1' \
    'moto<3' \
    'pytest<8' \
    'responses<0.22.0' \
    pyinotify>=0.9.4, \
    raven>=5.0.0

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
        -d /usr/local/lib/python3.9/site-packages/ \
        --no-deps; \
    done

ARG VERSION=unspecified
ENV VERSION=$VERSION

COPY version.txt /

WORKDIR /opt
ENTRYPOINT ["/opt/docker_entry.sh"]

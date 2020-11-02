FROM python:2.7-slim-buster

MAINTAINER brian <brian@planet.com>

ENV LANG C.UTF-8
ENV	LC_ALL C.UTF-8

# NB: gcc is only required to build the pyblake package, which does not ship as
# a wheel for linux. Once we move to python3 we can eliminate this dependency
# because hashlib supports blake2.
RUN apt-get update && \
    apt-get install --quiet --yes \
	gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# TODO: keep requirements in one place
RUN pip install \
    blinker>=1.4 \
    boto>=2.38.0 \
    boto3>=1.1.3 \
    click>=5.1 \
    Flask>=0.10.1 \
    flask-swagger==0.2.8 \
    memoized_property>=1.0.1 \
    pyblake2>=0.9.3 \
    python-dateutil>=2.4.2 \
    python-dotenv>=0.1.3 \
    pytz>=2015.4 \
    raven[flask]>=5.6.0 \
    requests>=2.5 \
    simplejson>=3.3.1 \
    six>=1.10.0 \
    # test requirements
    flake8==2.5.0 \
    freezegun==0.3.9 \
    moto==0.4.27 \
    pytest==3.0.2 \
    responses==0.5.0

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
        -d /usr/local/lib/python2.7/site-packages/ \
        --no-deps; \
    done

ARG VERSION=unspecified
ENV VERSION=$VERSION

COPY sha.txt /

WORKDIR /opt
ENTRYPOINT ["/opt/docker_entry.sh"]

#!/usr/bin/env bash
set -e
set -x

DYNAMODB_HOME=/opt/dynamodb
DYNAMODB_FILE=dynamodb_local_latest.tar.gz
DYNAMODB_URL=http://dynamodb-local.s3-website-us-west-2.amazonaws.com/${DYNAMODB_FILE}
DYNAMODB_JAR=DynamoDBLocal.jar
HERE=$(cd dirname $0; pwd)

mkdir -p ${DYNAMODB_HOME}
pushd ${DYNAMODB_HOME}

[ -f ${DYNAMODB_FILE} ] || curl -L -s ${DYNAMODB_URL} -o ${DYNAMODB_FILE}
[ -f ${DYNAMODB_JAR} ] || tar xzf ${DYNAMODB_FILE}
cp ${HERE}/dynamodb.sh ./

echo Launching dynamodb
CMD="start-stop-daemon --oknodo --start --pidfile /tmp/dynamodb.pid -b -m --startas ${DYNAMODB_HOME}/dynamodb.sh"
echo "#!/bin/sh -e" > /etc/rc.local
echo $CMD >> /etc/rc.local
/etc/init.d/rc.local start
popd

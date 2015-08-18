#!/usr/bin/env bash

DYNAMODB_HOME=~/opt/dynamodb
LOG=/var/log/dynamodb.log

echo '=========================' >> ${LOG}
date >> ${LOG}
cd ${DYNAMODB_HOME}
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb >> ${LOG}

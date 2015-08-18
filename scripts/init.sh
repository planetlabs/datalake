#!/usr/bin/env bash
exit 1
set -e
set -x

cd $(dirname $0)

./install_java.sh
./install_dynamodb.sh

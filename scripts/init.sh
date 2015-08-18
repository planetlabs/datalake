#!/usr/bin/env bash

set -e
set -x

cd $(dirname $0)

./install_java.sh
./install_dynamodb.sh

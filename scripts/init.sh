#!/usr/bin/env bash
set -e
set -x

cd $(dirname $0)

./install_java.sh
./install_dynamodb.sh
[ -z $(which git) ] && apt-get -y install git
[ -z $(which pip) ] && apt-get -y install python-pip

exit 0

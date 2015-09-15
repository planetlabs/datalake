#!/usr/bin/env bash
set -e
set -x

apt-get update
[ -z $(which git) ] && apt-get -y install git
[ -z $(which pip) ] && apt-get -y install python-pip python-dev
# install dynamodb local
wget -O - https://gist.githubusercontent.com/bcavagnolo/26cc0843a51d67b345c0/raw | bash

exit 0

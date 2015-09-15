#!/usr/bin/env bash
set -e
set -x

apt-get update
[ -z $(which git) ] && apt-get -y install git
[ -z $(which pip) ] && apt-get -y install python-pip python-dev

exit 0

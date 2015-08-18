#!/usr/bin/env bash
set -e
set -x

[ ! -z $(which java) ] && exit 0

export DEBIAN_FRONTEND=noninteractive
apt-get -y install openjdk-7-jre

#!/usr/bin/env bash

[ ! -z $(which java) ] && exit 0

export DEBIAN_FRONTEND=noninteractive
apt-get -y install openjdk-7-jre

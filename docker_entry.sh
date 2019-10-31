#!/bin/sh

HERE=$(cd `dirname "$0"` && pwd)

usage() {
    cat <<EOF
datalake docker entry script

Usage: docker_entry.sh <command> <args>

Valid commands include:

help: print this message

datalake: the datalake client

api: the datalake API server

ingester: the datalake ingester

EOF
}

case "$1" in
    "help")
        usage
        ;;
    "datalake")
        shift
        /usr/local/bin/datalake "$@"
        ;;
    "api")
        shift
        FLASK_APP=/opt/api/datalake_api/app.py flask run "$@"
        ;;
    "ingester")
        shift
        /usr/local/bin/datalake_tool "$@"
        ;;
    "")
        echo "ERROR: Please specify a command."
        usage
        exit 1
        ;;
    *)
        usage
        exit 1
esac

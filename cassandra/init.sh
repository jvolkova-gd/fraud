#!/bin/bash

for f in initdb/*; do
    case "$f" in
        *.cql)    echo "$0: running $f" && until cqlsh -f "$f"; do >&2 echo "Cassandra is unavailable - sleeping"; sleep 2; done & ;;
    esac
    echo
done

exec /docker-entrypoint.sh "$@"
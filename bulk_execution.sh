#!/usr/bin/env bash

set -e

urls=$(cat $1)
shift
cmd=$@

for url in $urls; do
    ssh $url $cmd
done

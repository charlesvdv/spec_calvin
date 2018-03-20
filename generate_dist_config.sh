#!/usr/bin/env bash

set -e

partition_counter=0
base_port=8000
urls=$(cat $1)
for line in $urls; do
    echo "node${partition_counter}=0:${partition_counter}:8:${line}:$((base_port+partition_counter))"
    partition_counter=$((partition_counter+1))
done

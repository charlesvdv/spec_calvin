#!/usr/bin/env bash

set -e

outputs=*output.txt

for file in $outputs; do
    partition=$(echo $file | egrep -o '^[0-9]+')
    latency=$(grep -A 1 '^LATENCY' $file | tail -n 1 | grep -oP '^([^,]+)')
    echo "${partition} latency: ${latency}"
done

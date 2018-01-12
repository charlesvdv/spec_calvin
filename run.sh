#!/usr/bin/env bash

DIR=$(dirname "$0")

export LD_LIBRARY_PATH="$DIR/ext/zookeeper/.libs:$DIR/ext/zeromq/src/.libs/:$LD_LIBRARY_PATH"
exec "$@"

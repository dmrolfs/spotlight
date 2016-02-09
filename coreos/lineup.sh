#!/bin/bash

echo "[lineup] starting lineup service..."

MAINCLASS="$1"
CP="$2"

shift
shift

echo "java -cp $CP $CONFIG $MAINCLASS"
java -cp "$CP" "$@" "$MAINCLASS"

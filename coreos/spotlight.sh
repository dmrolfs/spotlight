#!/bin/bash

echo "[spotlight] starting spotlight service..."

MAINCLASS="$1"
CP="$2"

shift
shift

echo "java -cp $CP $CONFIG $MAINCLASS"
java -cp "$CP" "$@" -XX:MaxMetaspaceSize=512m "$MAINCLASS"

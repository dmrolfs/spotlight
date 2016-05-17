#!/bin/bash

MAINCLASS="$1"
CP="$2"
CONFIG="config.resource=${SPOTLIGHT_CONFIG:-application.conf}"

echo "[spotlight] starting spotlight service using $CONFIG ..."

shift
shift

echo "java -cp $CP -D$CONFIG $MAINCLASS"

java -cp "$CP" -D"$CONFIG" "$@" -XX:MaxMetaspaceSize=512m "$MAINCLASS"

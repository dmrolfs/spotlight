#!/bin/bash

MAINCLASS="$1"
CP="$2"
ARGS="-c 2552"
#SPOTLIGHT_CONFIG="application-devdocker.conf"
echo "[spotlight] \$LOG_HOME = [$LOG_HOME]"
echo "[spotlight] \$SPOTLIGHT_CONFIG = [$SPOTLIGHT_CONFIG]"

CONFIG_RESOURCE="config.resource=${SPOTLIGHT_CONFIG}"
#echo "[spotlight] starting spotlight service using ${CONFIG_RESOURCE} ..."

shift
shift
echo "remaining args: $@"

echo "java -cp $CP -Dspotlight.config=$SPOTLIGHT_CONFIG -D$CONFIG_RESOURCE $@ -XX:MaxMetaspaceSize=512m $MAINCLASS $ARGS"

java -cp "$CP" -Dspotlight.config=${SPOTLIGHT_CONFIG} -D${CONFIG_RESOURCE} "$@" -XX:MaxMetaspaceSize=512m "$MAINCLASS" $ARGS

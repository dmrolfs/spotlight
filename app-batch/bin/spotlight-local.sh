#!/bin/bash

# DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR="$(dirname ${BASH_SOURCE[0]} )" 
echo "DIR=$DIR"

# mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

#SLF4J_LEVEL=DEBUG
#SLF4J_LEVEL=INFO
#SLF4J_LEVEL=WARN

CONFIG_RESOURCE=${SPOTLIGHT_CONFIG:=application.conf}

i=0
args=()

args[i]="--role ${ROLE:=all}"
((++i))

if [ ! "$EXTERNAL_HOSTNAME" = "" ]
then
  args[i]="--host \"${EXTERNAL_HOSTNAME}\""
  ((++i))
fi

if [ ! "$REQUESTED_EXTERNAL_PORT" = "" ]
then
  args[i]="--port ${REQUESTED_EXTERNAL_PORT}"
  ((++i))
fi

if [ ! "$BIND_HOSTNAME" = "" ]
then
  args[i]="--bind-hostname \"${BIND_HOSTNAME}\""
  ((++i))
fi

if [ ! "$BIND_PORT" = "" ]
then
  args[i]="--bind-port ${BIND_PORT}"
  ((++i))
fi

if [ ! "$BATCH_TARGET" = "" ]
then
  args[i]="${BATCH_TARGET}"
  ((++i))
fi

#rm ./log/monitor.csv
# rm -rf ./graphite/target/data/leveldb
# mkdir ./graphite/target/data/leveldb/shared-journal
# mkdir ./graphite/target/data/leveldb/snapshots

MAIN_CLASS="${1:-spotlight.app.FileBatchExample}"
shift

echo "running ${MAIN_CLASS} with environment variables:"
echo "  slf4j log level: ${SLF4J_LEVEL:-WARN}"
echo "  config resource: ${CONFIG_RESOURCE}"
echo "  role: ${ROLE}"
echo "  external hostname: ${EXTERNAL_HOSTNAME:-<nil>}"
echo "  requested external port: ${REQUESTED_EXTERNAL_PORT:-<nil>}"
echo "  bind hostname: ${BIND_HOSTNAME:-<nil>}"
echo "  bind port: ${BIND_PORT:-<nil>}"
echo "  batch target: ${BATCH_TARGET:-<nil>}"
echo "  env arguments: ${args[@]}"
echo "  remaining arguments: $@"
echo

echo "  java.library.path=$DIR/../../infr/native"
JAVAAGENT="$DIR/../../infr/coreos/aspectjweaver-1.8.10.jar"
echo "  javaagent=${JAVAAGENT}"
echo

CPATH="$DIR/../target/scala-2.12/*"
echo "CPATH=$CPATH"
echo

echo "java -classpath ${CPATH} -Dconfig.resource=${CONFIG_RESOURCE} -Djava.library.path=${DIR}/../../infr/native -DSLF4J_LEVEL=${SLF4J_LEVEL:-WARN} -javaagent:${JAVAAGENT} -XX:MaxMetaspaceSize=512m ${MAIN_CLASS} ${args[@]} $@"
echo

#java -classpath "${CPATH}" \
#  -Dconfig.resource=${CONFIG_RESOURCE} \
#  -DSLF4J_LEVEL="${SLF4J_LEVEL:-WARN}" \
#  -XX:MaxMetaspaceSize=512m \
#  ${MAIN_CLASS} ${args[@]} "$@"

java -classpath "${CPATH}" \
  -Dconfig.resource=${CONFIG_RESOURCE} \
  -Djava.library.path="${DIR}/../../infr/native" \
  -DSLF4J_LEVEL="${SLF4J_LEVEL:-WARN}" \
  -javaagent:"${JAVAAGENT}" \
  -XX:MaxMetaspaceSize=512m \
  ${MAIN_CLASS} ${args[@]} "$@"


#  -Xms4g \
#  -Xmx10g \

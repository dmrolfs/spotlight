#!/bin/bash

# DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR="$(dirname ${BASH_SOURCE[0]} )" 
echo "DIR=$DIR"

# mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

SPOTLIGHT_CONFIG="application.conf"

#SLF4J_LEVEL=DEBUG
#SLF4J_LEVEL=INFO
SLF4J_LEVEL=WARN


#rm ./log/monitor.csv
# rm -rf ./graphite/target/data/leveldb
# mkdir ./graphite/target/data/leveldb/shared-journal
# mkdir ./graphite/target/data/leveldb/snapshots

MAIN_CLASS="${1:-spotlight.app.FileBatchExample}"
shift

echo "running ${MAIN_CLASS}..."
echo "remaining arguments: $@"
echo "slf4j log level: ${SLF4J_LEVEL}"
echo

CPATH="$DIR/../target/scala-*/com.github.dmrolfs-spotlight-*.jar"
echo "CPATH=$CPATH"
echo "java.library.path=$DIR/../../infr/native"
JAVAAGENT="$DIR/../../infr/coreos/aspectjweaver-1.8.10.jar"

echo "javaagent=${JAVAAGENT}"

java -classpath ${CPATH} \
  -Dspotlight.config=${SPOTLIGHT_CONFIG} \
  -Dconfig.resource=${SPOTLIGHT_CONFIG} \
  -Djava.library.path="${DIR}/../../infr/native" \
  -DSLF4J_LEVEL="${SLF4J_LEVEL}" \
  -Xms4g \
  -Xmx10g \
  -javaagent:"${JAVAAGENT}" \
  -XX:MaxMetaspaceSize=512m \
  ${MAIN_CLASS} -c 2552 "$@"

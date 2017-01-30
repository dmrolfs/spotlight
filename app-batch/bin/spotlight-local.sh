#!/bin/bash

# DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR="$(dirname ${BASH_SOURCE[0]} )" 
echo "DIR=$DIR"

# mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

SPOTLIGHT_CONFIG="application.conf"

#rm ./log/monitor.csv
# rm -rf ./graphite/target/data/leveldb
# mkdir ./graphite/target/data/leveldb/shared-journal
# mkdir ./graphite/target/data/leveldb/snapshots

MAIN_CLASS="${1:-spotlight.app.FileBatchExample}"
shift

echo "running ${MAIN_CLASS}..."
echo "remaining arguments: $@"
echo

CPATH="$DIR/../target/scala-*/com.github.dmrolfs-spotlight-*.jar"
echo "CPATH=$CPATH"
echo "java.library.path=$DIR/../../infr/native"
echo "javaagent=$DIR/../../infr/coreos/aspectjweaver-1.8.8.jar"

java -classpath $CPATH \
  -Dspotlight.config=$SPOTLIGHT_CONFIG \
  -Dconfig.resource=$SPOTLIGHT_CONFIG \
  -Djava.library.path="$DIR/../../infr/native" \
  -DSLF4J_LEVEL=WARN \
  -Xms4g \
  -Xmx10g \
  -javaagent:"$DIR/../../infr/coreos/aspectjweaver-1.8.8.jar" \
  -XX:MaxMetaspaceSize=512m \
  ${MAIN_CLASS} -c 2552 "$@"
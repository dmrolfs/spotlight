#!/usr/bin/env bash

mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

SPOTLIGHT_CONFIG="application.conf"

#rm ./log/monitor.csv
rm -rf ./graphite/target/data/leveldb
mkdir ./graphite/target/data/leveldb/shared-journal
mkdir ./graphite/target/data/leveldb/snapshots

MAIN_CLASS="${1:-spotlight.app.GraphiteSpotlight}"
shift

echo "running ${MAIN_CLASS}..."
echo "remaining arguments: $@"
echo

CPATH="./graphite/target/scala-2.11/com.github.dmrolfs-spotlight-graphite-*.jar"
java -classpath $CPATH \
  -Dspotlight.config=$SPOTLIGHT_CONFIG \
  -Dconfig.resource=$SPOTLIGHT_CONFIG \
  -Djava.library.path=native \
  -Xms4g \
  -Xmx8g \
  -javaagent:graphite/coreos/aspectjweaver-1.8.8.jar \
  -XX:MaxMetaspaceSize=512m \
  ${MAIN_CLASS} -c 2552 "$@"



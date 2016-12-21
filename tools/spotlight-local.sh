#!/usr/bin/env bash

mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

SPOTLIGHT_CONFIG="application.conf"

#rm ./log/monitor.csv
rm -rf ./graphite/target/data/leveldb
mkdir ./graphite/target/data/leveldb/shared-journal
mkdir ./graphite/target/data/leveldb/snapshots

echo "running ${1:-spotlight.app.GraphiteSpotlight}..."
echo
echo

CPATH="./graphite/target/scala-2.11/com.github.dmrolfs-spotlight-graphite-*.jar"
java -classpath $CPATH \
  -Dspotlight.config=$SPOTLIGHT_CONFIG \
  -Dconfig.resource=$SPOTLIGHT_CONFIG \
  -Djava.library.path=native \
  -javaagent:graphite/coreos/aspectjweaver-1.8.8.jar \
  -XX:MaxMetaspaceSize=512m \
  ${1:-spotlight.app.GraphiteSpotlight} -c 2552


#  -Xms4096m \
#  -Xmx4096m \

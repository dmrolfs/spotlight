#!/usr/bin/env bash

mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

SPOTLIGHT_CONFIG="application.conf"

java -cp graphite/target/scala-2.11/com.github.dmrolfs-spotlight-graphite-2.0.4-SNAPSHOT.jar \
  -Dspotlight.config=$SPOTLIGHT_CONFIG \
  -Dconfig.resource=$SPOTLIGHT_CONFIG \
  -Djava.library.path=native \
  -javaagent:coreos/aspectjweaver-1.8.8.jar \
  -XX:MaxMetaspaceSize=512m \
  spotlight.app.GraphiteSpotlight -c 2552

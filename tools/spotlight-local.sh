#!/usr/bin/env bash

mkdir ./target/persistence/shared-journal ./target/persistence/snapshots

java -cp graphite/target/scala-2.11/com.github.dmrolfs-spotlight-graphite-2.0.3.jar \
  -Dconfig.resource=application.conf \
  -Djava.library.path=native \
  -javaagent:coreos/aspectjweaver-1.8.8.jar \
  -XX:MaxMetaspaceSize=512m \
  spotlight.app.GraphiteSpotlight -c 2552

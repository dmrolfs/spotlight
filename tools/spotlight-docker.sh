#!/usr/bin/env bash

docker run --rm --name spotlight \
-v ~/spotlight/data/journal:/var/lib/spotlight/data/leveldb/shared-journal \
-v ~/spotlight/data/snapshots/:/var/lib/spotlight/data/leveldb/snapshots \
-v ~/spotlight/log/:/var/log \
-v ~/spotlight/etc/:/etc/spotlight \
--env SPOTLIGHT_CONFIG=$SPOTLIGHT_CONFIG \
dmrolfs/spotlight-graphite:latest


#--entrypoint /bin/sh -it \

#!/usr/bin/env bash


/init/scripts/wait-for-it.sh -t 0 cassandra-node1:9042 -- echo "CASSANDRA Node1 started"
/init/scripts/wait-for-it.sh -t 0 cassandra-node2:9042 -- echo "CASSANDRA Node2 started"
/init/scripts/wait-for-it.sh -t 0 cassandra-node3:9042 -- echo "CASSANDRA Node3 started"

echo "### CASSANDRA INITIALISED! ###"


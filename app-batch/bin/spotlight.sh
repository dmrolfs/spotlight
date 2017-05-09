#!/bin/bash

set -e
set -x

#/app/scripts/wait-for-it.sh -t 0 cassandra-node1:9041 -- echo "CASSANDRA Node1 started"
#/app/scripts/wait-for-it.sh -t 0 cassandra-node2:9041 -- echo "CASSANDRA Node2 started"
#/app/scripts/wait-for-it.sh -t 0 cassandra-node3:9041 -- echo "CASSANDRA Node3 started"

# DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
dir="$(dirname ${BASH_SOURCE[0]} )"
echo "DIR=$dir"

# Usage info
show_help() {
cat << EOF
Usage: ${0##*/} [-r ROLE] [-h IP_ADDRESS] [-p PORT] [-b BIND_IP_ADDRESS] [-d BIND_PORT] [-l LEVEL] [-m MAIN_FQCN] [-c CONFIG_RESOURCE] [-x JVM_MAX_MEMORY] [PATH]...
Analyzes data files at PATH for anomalies

-?                    Display this help and exit

[-r ROLE]             Role node plays in spotlight cluster:
  all: used for single-node use where the node handles all responsibilities
  seed: one of required seed nodes who manage cluster membership
  analysis: algorithm worker processing
  intake: integration point with time series data supplier and executes the spotlight execution stream

[-h IP_ADDRESS]       The hostname or ip clients should connect to. InetAddress.getLocalHost.getHostAddress is used if empty

[-p PORT]             Listening remote port for this node in the processing cluster.
  There must be at least one seed at 2551 or 2552; otherwise can be 0 which is the default

[-H BIND_IP_ADDRESS]  Use this setting to bind a network interface to a different hostname or ip than remoting protocol expects messages
 at. Use "0.0.0.0" to bind to all interfaces. akka.remote.netty.tcp.hostname if empty

[-P BIND_PORT]        Use this setting to bind a network interface to a different port than remoting protocol expects messages at. This may be used when running akka nodes in a separated networks (under NATs or docker containers). Use 0 if you want a random available port. Examples:

 akka.remote.netty.tcp.port = 2552
 akka.remote.netty.tcp.bind-port = 2553
Network interface will be bound to the 2553 port, but remoting protocol will expect messages sent to port 2552.

 akka.remote.netty.tcp.port = 0
 akka.remote.netty.tcp.bind-port = 0
Network interface will be bound to a random port, and remoting protocol will expect messages sent to the bound port.

 akka.remote.netty.tcp.port = 2552
 akka.remote.netty.tcp.bind-port = 0
Network interface will be bound to a random port, but remoting protocol will expect messages sent to port 2552.

 akka.remote.netty.tcp.port = 0
 akka.remote.netty.tcp.bind-port = 2553
Network interface will be bound to the 2553 port, and remoting protocol will expect messages sent to the bound port.

 akka.remote.netty.tcp.port = 2552
 akka.remote.netty.tcp.bind-port = ""
Network interface will be bound to the 2552 port, and remoting protocol will expect messages sent to the bound port.

akka.remote.netty.tcp.port if empty

[-l LEVEL]            SLF4J log level WARN is default: OFF|ALL|TRACE\DEBUG\INFO\WARN\ERROR\FATAL

[-m MAIN_FQCN]        Fully qualified Java classname for the executable main class. spotlight.app.FileBatchExample is default

[-c CONFIG_RESOURCE]  Configuration resource file. application.conf is default

[ -L ]                Force node to execute locally in conjunction with -r all.
[-n JVM_MIN_MEMORY]   Minimum memory allocation for JVM. 1g is default
[-x JVM_MAX_MEMORY]   Maximum memory allocation for JVM. 2g is default
EOF
}

role=${ROLE:-all}
external_hostname=${EXTERNAL_HOSTNAME}
requested_external_port=${REQUESTED_EXTERNAL_PORT}
bind_hostname=${BIND_HOSTNAME}
bind_port=${BIND_PORT}
slf4j_level=${SLF4J_LEVEL:-WARN}
main_class=${MAIN_CLASS:-spotlight.app.FileBatchExample}
jvm_min_memory=${JVM_MIN_MEMORY:-1g}
jvm_max_memory=${JVM_MAX_MEMORY:-2g}
config_resource=${CONFIG_RESOURCE:-application.conf}
force_local=${FORCE_LOCAL:-false}

while getopts :?Lr:h:p:H:P:l:m:c:n:x: opt
do
  case "$opt" in
    r)
      echo "using $OPTARG for role" >&2
      role="$OPTARG"
      ;;
    h)
      echo "using $OPTARG for external_hostname" >&2
      external_hostname="$OPTARG"
      ;;
    p)
      echo "using $OPTARG for requested_external_port" >&2
      requested_external_port="$OPTARG"
      ;;
    H)
      echo "using $OPTARG for bind_hostname" >&2
      bind_hostname="$OPTARG"
      ;;
    P)
      echo "using $OPTARG for bind_port" >&2
      bind_port="$OPTARG"
      ;;
    l)
      echo "using $OPTARG for slf4j_log" >&2
      slf4j_level="$OPTARG"
      ;;
    m)
      echo "using $OPTARG for main_class" >&2
      main_class="$OPTARG"
      ;;
    c)
      echo "using $OPTARG for config_resource" >&2
      config_resource="$OPTARG"
      ;;
    n)
      echo "using $OPTARG for jvm_min_memory" >&2
      jvm_min_memory="$OPTARG"
      ;;
    x)
      echo "using $OPTARG for jvm_max_memory" >&2
      jvm_max_memory="$OPTARG"
      ;;
    L)
      echo "setting true for force_local" >&2
      force_local=true
      ;;
    ?)
      show_help
      exit 0
      ;;
  esac
done
echo "OPTIND: $OPTIND"
echo ${#@}
shift $(($OPTIND - 1))

default_batch_target="$dir/../data"
batch_target=${1-${default_batch_target}}
shift

i=0
args=()

args[i]="--role ${role}"
((++i))

if [ ! "$external_hostname" = "" ]
then
  args[i]="--host \"${external_hostname}\""
  ((++i))
fi

if [ ! "$requested_external_port" = "" ]
then
  args[i]="--port ${requested_external_port}"
  ((++i))
fi

if [ ! "$bind_hostname" = "" ]
then
  args[i]="--bind-hostname \"${bind_hostname}\""
  ((++i))
fi

if [ ! "$bind_port" = "" ]
then
  args[i]="--bind-port ${bind_port}"
  ((++i))
fi

if [ "$force_local" = true ]
then
  args[i]="--force-local"
  ((++i))
fi

echo "running ${main_class} with environment variables:"
echo "  role: ${role}"
echo "  external hostname: ${external_hostname:-<nil>}"
echo "  requested external port: ${requested_external_port:-<nil>}"
echo "  bind hostname: ${bind_hostname:-<nil>}"
echo "  bind port: ${bind_port:-<nil>}"
echo "  batch target: ${batch_target:-<nil>}"
echo "  slf4j log level: ${slf4j_level:-WARN}"
echo "  config resource: ${config_resource}"
echo "  force local: ${force_local}"
echo "  env arguments: ${args[@]}"
echo "  remaining arguments: $@"
echo

echo "  java.library.path=$dir/../../infr/native"
java_agent="$dir/../../infr/coreos/aspectjweaver-1.8.10.jar"
echo "  java_agent=${java_agent}"
echo

cpath="$dir/../target/scala-2.12/*"
echo "classpath=$cpath"
echo

echo "java -classpath ${cpath} -Dconfig.resource=${config_resource} -Djava.library.path=${dir}/../../infr/native -DSLF4J_LEVEL=${slf4j_level:-WARN} -javaagent:${java_agent} -XX:MaxMetaspaceSize=512m -Xms${jvm_min_memory:-1g} -Xmx${jvm_max_memory:-2g} -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xloggc:gc_g1_4g_5_5000_1000_60.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps ${main_class:-spotlight.app.FileBatchExample} ${args[@]} ${batch_target} $@"
echo

java -classpath "${cpath}" \
  -Dconfig.resource="${config_resource}" \
  -Djava.library.path="${dir}/../../infr/native" \
  -DSLF4J_LEVEL="${slf4j_level:-WARN}" \
  -javaagent:"${java_agent}" \
  -XX:MaxMetaspaceSize=512m \
  -Xms${jvm_min_memory:-1g} \
  -Xmx${jvm_max_memory:-2g} \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=100 \
  -Xloggc:gc_g1_4g_5_5000_1000_60.log \
  -XX:+PrintGCDetails \
  -XX:+PrintGCDateStamps \
  -XX:+PrintGCTimeStamps \
  ${main_class:-spotlight.app.FileBatchExample} ${args[@]} "${batch_target}" "$@"

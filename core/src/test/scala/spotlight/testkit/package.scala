package spotlight

import com.typesafe.config.{ Config, ConfigFactory }
import omnibus.commons.identifier.ShortUUID

/** Created by rolfsd on 7/5/16.
  */
package object testkit {
  def config( rootDir: String = ".", systemName: String ): Config = {
    val testConfig = {
      val algoJournal = "${inmemory-journal}"
      val algoSnapshot = "${inmemory-snapshot-store}"

      ConfigFactory.parseString(
        s"""
        |cassandra-snapshot-store {}
        |akka.persistence {
        |  algorithm.journal.plugin = ${algoJournal}
        |  algorithm.snapshot.plugin = ${algoSnapshot}
        |
        |  journal.plugin = "inmemory-journal"
        |  snapshot.plugin = "inmemory-snapshot-store"
        |
        |#  journal.leveldb-shared.store {
        |#    # DO NOT USE 'native = off' IN PRODUCTION !!!
        |#    native = off
        |#    dir = "${rootDir}/target/shared-journal"
        |#  }
        |  journal.leveldb {
        |    # DO NOT USE 'native = off' IN PRODUCTION !!!
        |    native = off
        |    dir = "${rootDir}/target/journal/${ShortUUID()}"
        |  }
        |  snapshot-store.local.dir = "${rootDir}/target/snapshots"
        |}
        |
        |akka.persistence.algorithm.journal.plugin {
        |  class = "akka.persistence.inmemory.journal.InMemoryAsyncWriteJournal"
        |  # ask timeout on Futures
        |  ask-timeout = "10s"
        |
        |  plugin-dispatcher = "akka.actor.default-dispatcher"
        |  recovery-event-timeout = 60m
        |  circuit-breaker {
        |    max-failures = 10
        |    call-timeout = 600s
        |    reset-timeout = 30s
        |  }
        |  replay-filter {
        |    # What the filter should do when detecting invalid events.
        |    # Supported values:
        |    # `repair-by-discard-old` : discard events from old writers,
        |    #                           warning is logged
        |    # `fail` : fail the replay, error is logged
        |    # `warn` : log warning but emit events untouched
        |    # `off` : disable this feature completely
        |    mode = repair-by-discard-old
        |
        |    # It uses a look ahead buffer for analyzing the events.
        |    # This defines the size (in number of events) of the buffer.
        |    window-size = 100
        |
        |    # How many old writerUuid to remember
        |    max-old-writers = 10
        |
        |    # Set this to `on` to enable detailed debug logging of each
        |    # replayed event.
        |    debug = off
        |  }
        |}
        |
        |akka {
        |#  loggers = ["akka.event.slf4j.Slf4jLogger", "akka.testkit.TestEventListener"]
        |#  logging-filter = "akka.event.DefaultLoggingFilter"
        |#  loglevel = DEBUG
        |#  stdout-loglevel = "DEBUG"
        |#  log-dead-letters = on
        |#  log-dead-letters-during-shutdown = on
        |
        |  actor {
        |#    provider = "akka.cluster.ClusterActorRefProvider"
        |  }
        |
        |  remote {
        |    log-remote-lifecycle-events = off
        |    netty.tcp {
        |      hostname = "127.0.0.1"
        |      port = 0
        |    }
        |  }
        |
        |  cluster {
        |    seed-nodes = [
        |      "akka.tcp://${systemName}@127.0.0.1:2551",
        |      "akka.tcp://${systemName}@127.0.0.1:2552"
        |    ]
        |
        |    auto-down-unreachable-after = 10s
        |  }
        |}
        |
        |akka.actor.debug {
        |  # enable function of Actor.loggable(), which is to log any received message
        |  # at DEBUG level, see the “Testing Actor Systems” section of the Akka
        |  # Documentation at http://akka.io/docs
        |#  receive = on
        |
        |  # enable DEBUG logging of all AutoReceiveMessages (Kill, PoisonPill et.c.)
        |#  autoreceive = on
        |
        |  # enable DEBUG logging of actor lifecycle changes
        |#  lifecycle = on
        |
        |  # enable DEBUG logging of all LoggingFSMs for events, transitions and timers
        |#  fsm = on
        |
        |  # enable DEBUG logging of subscription changes on the eventStream
        |#  event-stream = on
        |
        |  # enable DEBUG logging of unhandled messages
        |#  unhandled = on
        |
        |  # enable WARN logging of misconfigured routers
        |#  router-misconfiguration = on
        |}
        |
        |demesne.register-dispatcher {
        |  type = Dispatcher
        |  executor = "fork-join-executor"
        |  fork-join-executor {
        |    # Min number of threads to cap factor-based parallelism number to
        |    parallelism-min = 2
        |    # Parallelism (threads) ... ceil(available processors * factor)
        |    parallelism-factor = 2.0
        |    # Max number of threads to cap factor-based parallelism number to
        |    parallelism-max = 10
        |  }
        |  # Throughput defines the maximum number of messages to be
        |  # processed per actor before the thread jumps to the next actor.
        |  # Set to 1 for as fair as possible.
        |  throughput = 100
        |}
        |
        |spotlight.dispatchers {
        |  outlier-detection-dispatcher {
        |    type = Dispatcher
        |    executor = "fork-join-executor"
        |    #  throughput = 100
        |    fork-join-executor { }
        |  }
        |
        |  publishing-dispatcher {
        |    type = Dispatcher
        |    executor = "thread-pool-executor"
        |    thread-pool-executor {
        |      fixed-pool-size = 8
        |    }
        |  }
        |}
        |
        |inmemory-journal {
        |  class = "akka.persistence.inmemory.journal.InMemoryAsyncWriteJournal"
        |  # ask timeout on Futures
        |  ask-timeout = "10s"
        |}
        |
        |# the akka-persistence-snapshot-store in use
        |inmemory-snapshot-store {
        |  class = "akka.persistence.inmemory.snapshot.InMemorySnapshotStore"
        |  # ask timeout on Futures
        |  ask-timeout = "10s"
        |}
        |
        |inmemory-read-journal {
        |  # Implementation class of the InMemory ReadJournalProvider
        |  class = "akka.persistence.inmemory.query.InMemoryReadJournalProvider"
        |
        |  # Absolute path to the write journal plugin configuration section to get the event adapters from
        |  write-plugin = "inmemory-journal"
        |
        |  # there are two modes; 'sequence' or 'uuid'.
        |  #
        |  # | mode     | offset        | description                                      |
        |  # | -------- | --------------| ------------------------------------------------ |
        |  # | sequence | NoOffset      | the query will return Sequence offset types      |
        |  # | sequence | Sequence      | the query will return Sequence offset types      |
        |  # | uuid     | NoOffset      | the query will return TimeBasedUUID offset types |
        |  # | uuid     | TimeBasedUUID | the query will return TimeBasedUUID offset types |
        |  #
        |  offset-mode = "sequence"
        |
        |  # ask timeout on Futures
        |  ask-timeout = "10s"
        |
        |  # New events are retrieved (polled) with this interval.
        |  refresh-interval = "100ms"
        |
        |  # How many events to fetch in one query (replay) and keep buffered until they
        |  # are delivered downstreams.
        |  max-buffer-size = "100"
        |}
        |
      """.stripMargin
      )
    }

    testConfig.resolve().withFallback( ConfigFactory.load( "reference.conf" ) )
  }
  //  def config( rootDir: String = "." ): Config = ConfigFactory.parseString(
  //    s"""
  //     |akka.loggers = ["akka.testkit.TestEventListener"]
  //     |
  //     |akka.persistence {
  //     |#  journal.plugin = "akka.persistence.journal.leveldb-shared"
  //     |  journal.plugin = "akka.persistence.journal.leveldb"
  //     |  journal.leveldb-shared.store {
  //     |    # DO NOT USE 'native = off' IN PRODUCTION !!!
  //     |    native = off
  //     |    dir = "target/shared-journal"
  //     |  }
  //     |  journal.leveldb {
  //     |    # DO NOT USE 'native = off' IN PRODUCTION !!!
  //     |    native = off
  //     |    dir = "${rootDir}/target/journal/${ShortUUID()}"
  //     |  }
  //     |  snapshot-store.local.dir = "target/snapshots"
  //     |}
  //     |
  //     |#akka {
  //     |#  persistence {
  //     |#    journal.plugin = "inmemory-journal"
  //     |#    snapshot-store.plugin = "inmemory-snapshot-store"
  //     |#
  //     |#    journal.plugin = "akka.persistence.journal.leveldb"
  //     |#    journal.leveldb.dir = "target/journal"
  //     |#    journal.leveldb.native = off
  //     |#    snapshot-store.plugin = "akka.persistence.snapshot-store.local"
  //     |#    snapshot-store.local.dir = "target/snapshots"
  //     |#  }
  //     |#}
  //     |
  //     |akka {
  //     |  loggers = ["akka.event.slf4j.Slf4jLogger"]
  //     |  logging-filter = "akka.event.DefaultLoggingFilter"
  //     |  loglevel = DEBUG
  //     |  stdout-loglevel = "DEBUG"
  //     |  log-dead-letters = on
  //     |  log-dead-letters-during-shutdown = on
  //     |
  //     |  actor {
  //     |    provider = "akka.cluster.ClusterActorRefProvider"
  //     |  }
  //     |
  //     |  remote {
  //     |    log-remote-lifecycle-events = off
  //     |    netty.tcp {
  //     |      hostname = "127.0.0.1"
  //     |      port = 0
  //     |    }
  //     |  }
  //     |
  //     |  cluster {
  //     |    seed-nodes = [
  //     |      "akka.tcp://ClusterSystem@127.0.0.1:2551",
  //     |      "akka.tcp://ClusterSystem@127.0.0.1:2552"
  //     |    ]
  //     |
  //     |    auto-down-unreachable-after = 10s
  //     |  }
  //     |}
  //     |
  //     |akka {
  //     |  extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension $"]
  //     |  actor {
  //     |    kryo  {
  //     |      type = "graph"
  //     |      idstrategy = "incremental"
  //     |      buffer-size = 4096
  //     |      max-buffer-size = -1
  //     |      use-manifests = false
  //     |      use-unsafe = false
  //     |      post-serialization-transformations = "lz4,aes"
  //     |      encryption {
  //     |        aes {
  //     |          mode = "AES/CBC/PKCS5Padding"
  //     |          key = j68KkRjq21ykRGAQ
  //     |          IV-length = 16
  //     |          custom-key-class = "sandbox.KyroCryptoKey"
  //     |        }
  //     |      }
  //     |      implicit-registration-logging = true
  //     |      kryo-trace = false
  //     |//      kryo-custom-serializer-init = "CustomKryoSerializerInitFQCN"
  //     |      resolve-subclasses = false
  //     |//      mappings {
  //     |//        "package1.name1.className1" = 20,
  //     |//        "package2.name2.className2" = 21
  //     |//      }
  //     |      classes = [
  //     |        "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$Advanced",
  //     |        "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$ConfigurationChanged"
  //     |      ]
  //     |    }
  //     |
  //     |    serializers {
  //     |      java = "akka.serialization.JavaSerializer"
  //     |      # Define kryo serializer
  //     |      kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
  //     |    }
  //     |
  //     |    serialization-bindings {
  //     |      "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$Advanced" = kryo
  //     |      "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$ConfigurationChanged" = kryo
  //     |    }
  //     |  }
  //     |}
  //     |
  //     |akka.actor.debug {
  //     |  # enable function of Actor.loggable(), which is to log any received message
  //     |  # at DEBUG level, see the “Testing Actor Systems” section of the Akka
  //     |  # Documentation at http://akka.io/docs
  //     |  receive = on
  //     |
  //     |  # enable DEBUG logging of all AutoReceiveMessages (Kill, PoisonPill et.c.)
  //     |  autoreceive = on
  //     |
  //     |  # enable DEBUG logging of actor lifecycle changes
  //     |  lifecycle = on
  //     |
  //     |  # enable DEBUG logging of all LoggingFSMs for events, transitions and timers
  //     |  fsm = on
  //     |
  //     |  # enable DEBUG logging of subscription changes on the eventStream
  //     |  event-stream = on
  //     |
  //     |  # enable DEBUG logging of unhandled messages
  //     |  unhandled = on
  //     |
  //     |  # enable WARN logging of misconfigured routers
  //     |  router-misconfiguration = on
  //     |}
  //     |
  //     |demesne.register-dispatcher {
  //     |  type = Dispatcher
  //     |  executor = "fork-join-executor"
  //     |  fork-join-executor {
  //     |    # Min number of threads to cap factor-based parallelism number to
  //     |    parallelism-min = 2
  //     |    # Parallelism (threads) ... ceil(available processors * factor)
  //     |    parallelism-factor = 2.0
  //     |    # Max number of threads to cap factor-based parallelism number to
  //     |    parallelism-max = 10
  //     |  }
  //     |  # Throughput defines the maximum number of messages to be
  //     |  # processed per actor before the thread jumps to the next actor.
  //     |  # Set to 1 for as fair as possible.
  //     |  throughput = 100
  //     |}
  //    """.stripMargin
  //  )
}

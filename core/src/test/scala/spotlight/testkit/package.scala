package spotlight

import com.typesafe.config.{Config, ConfigFactory}
import peds.commons.identifier.ShortUUID


/**
  * Created by rolfsd on 7/5/16.
  */
package object testkit {
  def config( rootDir: String = ".", systemName: String ): Config = {
    val testConfig = ConfigFactory.parseString(
      s"""
        |akka.persistence {
        |#  journal.plugin = "akka.persistence.journal.leveldb-shared"
        |  journal.plugin = "akka.persistence.journal.leveldb"
        |  journal.leveldb-shared.store {
        |    # DO NOT USE 'native = off' IN PRODUCTION !!!
        |    native = off
        |    dir = "${rootDir}/target/shared-journal"
        |  }
        |  journal.leveldb {
        |    # DO NOT USE 'native = off' IN PRODUCTION !!!
        |    native = off
        |    dir = "${rootDir}/target/journal/${ShortUUID()}"
        |  }
        |  snapshot-store.local.dir = "${rootDir}target/snapshots"
        |}
        |
        |akka {
        |  loggers = ["akka.event.slf4j.Slf4jLogger", "akka.testkit.TestEventListener"]
        |  logging-filter = "akka.event.DefaultLoggingFilter"
        |  loglevel = DEBUG
        |  stdout-loglevel = "DEBUG"
        |  log-dead-letters = on
        |  log-dead-letters-during-shutdown = on
        |
        |  actor {
        |    provider = "akka.cluster.ClusterActorRefProvider"
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
        |  receive = on
        |
        |  # enable DEBUG logging of all AutoReceiveMessages (Kill, PoisonPill et.c.)
        |  autoreceive = on
        |
        |  # enable DEBUG logging of actor lifecycle changes
        |  lifecycle = on
        |
        |  # enable DEBUG logging of all LoggingFSMs for events, transitions and timers
        |  fsm = on
        |
        |  # enable DEBUG logging of subscription changes on the eventStream
        |  event-stream = on
        |
        |  # enable DEBUG logging of unhandled messages
        |  unhandled = on
        |
        |  # enable WARN logging of misconfigured routers
        |  router-misconfiguration = on
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
      """.stripMargin
    )

    testConfig.withFallback( ConfigFactory.load( "reference.conf" ) )
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
//     |      "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$Advanced" = kyro
//     |      "spotlight.analysis.outlier.algorithm.AlgorithmProtocol$ConfigurationChanged" = kyro
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

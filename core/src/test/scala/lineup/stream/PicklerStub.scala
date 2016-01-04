package com.codahale.metrics.graphite

import com.codahale.metrics.graphite.PickledGraphite.MetricTuple

import scala.collection.JavaConverters._
import akka.util.ByteString


class PicklerStub extends PickledGraphite(null) {
  def pickle( metrics: (String, Long, String)* ): ByteString = {
    val tuples = metrics.toList map { case(n, t, v) => new MetricTuple( n, t, v ) }
    val result = super.pickleMetrics( tuples.asJava )
    ByteString( result )
  }
}

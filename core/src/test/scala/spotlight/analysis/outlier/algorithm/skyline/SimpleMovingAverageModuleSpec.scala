package spotlight.analysis.outlier.algorithm.skyline

import scala.concurrent.duration._
import akka.testkit._
import com.typesafe.scalalogging.LazyLogging
import demesne.AggregateRootModule
import org.scalatest.concurrent.ScalaFutures
import demesne.testkit.AggregateRootSpec
import org.scalatest.Tag
import spotlight.analysis.outlier.algorithm.AlgorithmModule.{Protocol => P}
import spotlight.model.outlier.OutlierPlan
import akka.actor.ActorSystem
import peds.commons.identifier.TaggedID
import shapeless.syntax.typeable._
import spotlight.analysis.outlier.algorithm.AlgorithmModule


/**
  * Created by rolfsd on 6/9/16.
  */
class SimpleMovingAverageModuleSpec extends AggregateRootSpec[SimpleMovingAverageModuleSpec] with ScalaFutures with LazyLogging {
  class Fixture extends AggregateFixture {
    val bus = TestProbe()
    system.eventStream.subscribe( bus.ref, classOf[AlgorithmModule.Event] )

    val pid = OutlierPlan.nextId()
    val id: TaggedID[OutlierPlan.Scope] = OutlierPlan.Scope( plan = "TestPlan", topic = "TestTopic", planId = pid )
    logger.info( "Fixture: DomainModel=[{}]",model, model )
    lazy val sma = SimpleMovingAverageModule aggregateOf id

    override def moduleCompanions: List[AggregateRootModule[_]] = List( SimpleMovingAverageModule )
    logger.debug( "Fixture.context = [{}]", context )
    logger.debug( "checkSystem elems: system:[{}] raw:[{}]", context.get(demesne.SystemKey).flatMap{_.cast[ActorSystem]}, context.get(demesne.SystemKey) )
  }

  override def createAkkaFixture(): Fixture = new Fixture

  object WIP extends Tag( "wip" )

  SimpleMovingAverageModule.algorithm.label.name should {
    "add algorithm" taggedAs (WIP) in { f: Fixture =>
      import f._
      sma ! P.Add( id )
      bus.expectMsgPF( max = 200.millis.dilated, hint = "algo added" ) {
        case p: P.Added => p.sourceId mustBe id
      }
    }

//    "must not respond before add" in { f: Fixture =>
//      import f._
//pending
//    }
  }
}


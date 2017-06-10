package spotlight.checkpoint

import scala.collection.immutable
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage._
import com.persist.logging._
import enumeratum._


trait ValveSwitch {
  def open: Unit
  def close: Unit
}

/** Created by rolfsd on 6/8/17 from gist by regis leray
  */
object Valve {
  sealed trait Mode extends EnumEntry

  object Mode extends Enum[Mode] {
    override def values: immutable.IndexedSeq[Mode] = findValues

    case object Open extends Mode
    case object Closed extends Mode
  }
}

class Valve[A]( startMode: Valve.Mode = Valve.Mode.Open )
    extends GraphStageWithMaterializedValue[FlowShape[A, A], ValveSwitch] with ClassLogging {

  val in = Inlet[A]( "valve.in" )
  val out = Outlet[A]( "valve.out" )
  override val shape: FlowShape[A, A] = FlowShape( in, out )

  override def createLogicAndMaterializedValue( inheritedAttributes: Attributes ): ( GraphStageLogic, ValveSwitch ) = {
    val logic = new ValveGraphStageLogic( startMode )
    ( logic, logic.switch )
  }

  private class ValveGraphStageLogic( var mode: Valve.Mode ) extends GraphStageLogic( shape ) with InHandler with OutHandler {
    setHandlers( in, out, this )

    private var bufferedElement: Option[A] = None

    override def onPush(): Unit = {
      val element = grab( in )
      mode match {
        case Valve.Mode.Open ⇒ push( out, element )
        case Valve.Mode.Closed ⇒ bufferedElement = Some( element )
      }
    }

    override def onPull(): Unit = pull( in )

    val switch: ValveSwitch = new ValveSwitch {
      val callback: AsyncCallback[A] = getAsyncCallback[A] { push( out, _ ) }

      override def open: Unit = {
        mode = Valve.Mode.Open
        bufferedElement foreach callback.invoke
        bufferedElement = None
      }

      override def close: Unit = { mode = Valve.Mode.Closed }
    }
  }
}

//      setHandler(
//        in,
//        new InHandler {
//          override def onPush(): Unit = {
//            grab( in ) match {
//              case c @ Checkpoint( token, initiator ) => {
//                val start = joda.DateTime.now
//
//                dispatchCheckpoint( c )
//                .onComplete {
//                  case Success( c ) => {
//                    val leadTime = leadTimeFrom( start )
//                    log.debug(
//                      Map(
//                        "@msg" -> "checkpoint success",
//                        "checkpoint" -> Map(
//                          "timestamp" -> start.show,
//                          "lead-time" -> leadTime.show,
//                          "token" -> token,
//                          "initiator" -> initiator.show
//                        )
//                      )
//                    )
//
//                    initiator ! CheckpointSuccess( start, leadTime, token )
//                    pull( in )
//                  }
//
//                  case Failure( ex ) => {
//                    val leadTime = leadTimeFrom( start )
//
//                    log.error(
//                      Map(
//                        "@msg" -> "checkpoint failure",
//                        "checkpoint" -> Map(
//                          "timestamp" -> start.show,
//                          "error-lead-time" -> leadTime.show,
//                          "token" -> token,
//                          "initiator" -> initiator.show
//                        )
//                      ),
//                      ex
//                    )
//
//                    initiator ! CheckpointFailure( start, leadTime, token, ex )
//                    pull( in )
//                  }
//                }
//              }
//
//              case e => push( out, e )
//            }
//
//          }
//        }
//      )
//
//      setHandler( out, new OutHandler { override def onPull(): Unit = pull( in ) } )
//
//      def leadTimeFrom( start: joda.DateTime ): FiniteDuration = {
//        FiniteDuration( System.currentTimeMillis() - start.getMillis, MILLISECONDS )
//      }
//
//      type COp[A, R] = Kleisli[Future, A, R]
//
//      val findSavePoints: COp[Checkpoint, Set[Algorithm.TID]] = Kleisli[Future, Checkpoint, Set[Algorithm.TID]] { c => ??? }
//
////      var dispatched: Set[Algorithm.TID] = Set.empty[Algorithm.TID]
////      var responses: Set[SavePointResponse] = Set.empty[SavePointResponse]
//
//      val dispatch: COp[(Checkpoint, Map[AggregateRootType, Set[Algorithm.TID]]), Map[ActorRef, SavePointResponse]] = {
//        Kleisli[Future, (Checkpoint, Map[AggregateRootType, Set[Algorithm.TID]]), Map[ActorRef, SavePointResponse]] {
//          case (c, algorithmRootIds) => {
//            val savepoints: Set[(Algorithm.TID, Future[])] = for {
//              rootIds <- algorithmRootIds.toSet
//              (rt, tids) = rootIds
//              tid <- tids
//            } yield {
//              val ref = model( rt, tid.id )
//              val cmd = rt.snapshot map { _ saveSnapshotCommand tid } getOrElse { SaveSna}
//              ref.sendEnvelope( demesne.SaveSnapshot( tid ) )( c.sourceRef )
//              ( tid, savepoint )
//            }
//          }
//        }
//      }
//
//      val reduce: COp[(Checkpoint, Map[ActorRef, SavePointResponse]), (Checkpoint, CheckpointResponse)] = {
//        Kleisli[Future, (Checkpoint, Map[ActorRef, SavePointResponse]), (Checkpoint, CheckpointResponse)] {
//          case (c, saveResponses) => Future successful {
//            val failures = saveResponses.values collect { case r: SavePointFailure => SavePointError( r.savePoint, r.error ) }
//
//            val result = {
//              val leadTime = leadTimeFrom( c.start )
//              if ( failures.isEmpty ) CheckpointSuccess( c.start, leadTime, c.token )
//              else {
//                CheckpointFailure( c.start, leadTime, c.token, NonEmptyList(failures.head, failures.tail.toList) )
//              }
//            }
//
//            ( c, result )
//          }
//        }
//      }
//
//      type CReqResp = (Checkpoint, CheckpointResponse)
//      val logReponse: COp[CReqResp, CReqResp] = Kleisli[Future, CReqResp, CReqResp] { case (req, resp) =>
//        Future successful {
//          val cpBase = Map( "start" -> resp.start.show, "token" -> resp.token, "source" -> req.sourceRef )
//
//          resp match {
//            case success: CheckpointSuccess => {
//              log.debug(
//                Map(
//                  "@msg" -> "checkpoint success",
//                  "checkpoint" -> ( cpBase + ("lead-time" -> success.leadTime.show) )
//                )
//              )
//            }
//
//            case failure: CheckpointFailure => {
//              log.error(
//                Map(
//                  "@msg" -> "checkpoint failure",
//                  "checkpoint" -> ( cpBase + ("error-lead-time" -> resp.leadTime.show) ),
//                  "failures" -> failure.show
//                )
//              )
//            }
//          }
//
//          (req, resp)
//        }
//      }
//
//      val notifyAndPull: COp[CReqResp, Done] = Kleisli[Future, CReqResp, Done] { case (req, resp) =>
//        Future successful {
//          req.sourceRef ! resp
//          pull( in )
//          Done
//        }
//      }

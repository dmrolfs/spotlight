package spotlight

import akka.Done
import demesne.StartTask


package object infrastructure {
  val kamonStartTask: StartTask = StartTask.withFunction( "start Kamon monitoring" ){ bc => kamon.Kamon.start(); Done }
}

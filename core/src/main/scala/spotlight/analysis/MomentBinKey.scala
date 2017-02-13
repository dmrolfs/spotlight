package spotlight.analysis

import scalaz.Validation
import peds.commons.Valid

/** Created by rolfsd on 9/21/16.
  */
class MomentBinKey {
  case class MomentBinKey( dayOfWeek: DayOfWeek, hourOfDay: Int ) {
    def id: String = s"${dayOfWeek.label}:${hourOfDay}"
  }

  import org.joda.{ time ⇒ joda }

  sealed trait DayOfWeek {
    def label: String
    def jodaKey: Int
  }

  object DayOfWeek {
    def fromJodaKey( key: Int ): Valid[DayOfWeek] = Validation.fromTryCatchNonFatal { JodaDays( key ) }.toValidationNel

    val JodaDays: Map[Int, DayOfWeek] = Map(
      Seq( Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday ).map { d ⇒ d.jodaKey → d }: _*
    )
  }

  case object Sunday extends DayOfWeek {
    override val label: String = "Sunday"
    override val jodaKey: Int = joda.DateTimeConstants.SUNDAY
  }
  case object Monday extends DayOfWeek {
    override val label: String = "Monday"
    override val jodaKey: Int = joda.DateTimeConstants.MONDAY
  }
  case object Tuesday extends DayOfWeek {
    override val label: String = "Tuesday"
    override val jodaKey: Int = joda.DateTimeConstants.TUESDAY
  }
  case object Wednesday extends DayOfWeek {
    override val label: String = "Wednesday"
    override val jodaKey: Int = joda.DateTimeConstants.WEDNESDAY
  }
  case object Thursday extends DayOfWeek {
    override val label: String = "Thursday"
    override val jodaKey: Int = joda.DateTimeConstants.THURSDAY
  }
  case object Friday extends DayOfWeek {
    override val label: String = "Friday"
    override val jodaKey: Int = joda.DateTimeConstants.FRIDAY
  }
  case object Saturday extends DayOfWeek {
    override val label: String = "Saturday"
    override val jodaKey: Int = joda.DateTimeConstants.SATURDAY
  }

}

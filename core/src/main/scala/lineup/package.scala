import scalaz.{ NonEmptyList, \/ }
import scalaz._, Scalaz._


package object lineup {
  type Valid[T] = \/[NonEmptyList[Throwable], T]
  object Valid {
    implicit class MarkValid[T]( val underlying: T ) extends AnyVal {
      def valid: Valid[T] = underlying.right
      def invalid( reason: Throwable ): Valid[T] = NonEmptyList(reason).left
    }
  }
}

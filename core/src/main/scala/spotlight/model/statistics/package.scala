package spotlight.model

import scala.collection.immutable

/** Created by rolfsd on 3/8/17.
  */
package object statistics {
  type CircularBuffer[A] = immutable.Vector[A]
  object CircularBuffer {
    def addTo[A]( width: Int )( buffer: CircularBuffer[A], elem: A ): CircularBuffer[A] = {
      if ( width > 0 ) { buffer.drop( buffer.size - width + 1 ) :+ elem } else { buffer }
    }

    def empty[A]: CircularBuffer[A] = immutable.Vector.empty[A]
  }
}

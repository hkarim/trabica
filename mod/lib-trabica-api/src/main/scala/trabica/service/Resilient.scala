package trabica.service

import cats.effect.IO

import scala.concurrent.duration.*

object Resilient {

  def retry[A](io: IO[A], initial: Int, max: Int): IO[A] =
    io.handleErrorWith {
      case e: java.io.IOException =>
        if initial <= max then {
          IO.println("retrying .. ") *>
            IO.sleep(3.seconds) *>
            retry(io, initial + 1, max)
        } else {
          IO.println("giving up") *>
            IO.raiseError(e)
        }
    }

}

package trabica.service

import cats.effect.IO
import trabica.model.NodeError

import scala.concurrent.duration.*

object Resilient {

  private final val logger = scribe.cats[IO]

  private def loop[A](e: Throwable, prefix: String, io: IO[A], initial: Int, max: Int): IO[A] =
    if initial <= max then {
      logger.debug(s"$prefix `${e.getMessage}`, retrying [$initial].. ") >>
        IO.sleep(1.seconds) >>
        retry(io, initial + 1, max)
    } else {
      logger.info("giving up, raising error") >>
        IO.raiseError(e)
    }

  def retry[A](io: IO[A], initial: Int, max: Int): IO[A] =
    io.handleErrorWith {
      case NodeError.SocketReadError(e) =>
        loop(e, "captured socket read error", io, initial, max)
      case NodeError.SocketWriteError(e) =>
        loop(e, "captured socket write error", io, initial, max)
      case e: java.net.ConnectException =>
        loop(e, "captured socket connection error", io, initial, max)
      case e: java.nio.channels.ClosedChannelException =>
        loop(e, "captured closed channel error", io, initial, max)
      case e =>
        logger.error("unable to handle error: ", e) >>
          IO.raiseError(e)
    }

}

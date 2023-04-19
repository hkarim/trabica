package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import com.comcast.ip4s.*
import io.circe.*
import io.circe.syntax.*
import fs2.*
import fs2.concurrent.*
import fs2.io.net.{Network, Socket}
import trabica.context.NodeContext
import trabica.model.*

import scala.concurrent.duration.*

class VoteStream(nodeContext: NodeContext) {

  def run: IO[Unit] =
    for {
      signal   <- SignallingRef.of[IO, Boolean](false)
      peerIp   <- host("trabica.peer.ip")
      peerPort <- port("trabica.peer.port")
      address = SocketAddress(peerIp, peerPort)
      socket <- retry(socketStream(address, signal), 0, 10)
    } yield socket

  private def retry[A](io: IO[A], initial: Int, max: Int): IO[A] =
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

  private def host(key: String): IO[Host] =
    IO.fromOption(Host.fromString(nodeContext.config.getString(key)))(
      new IllegalArgumentException(s"invalid host value for `$key`")
    )

  private def port(key: String): IO[Port] =
    IO.fromOption(Port.fromInt(nodeContext.config.getInt(key)))(
      new IllegalArgumentException(s"invalid port value for `$key`")
    )

  private def pipe(signal: Signal[IO, Boolean], socket: Socket[IO]): IO[Unit] = {
    val writes: IO[Unit] =
      Stream
        .awakeEvery[IO](2.seconds)
        .evalTap(_ => IO.println("writes up ...."))
        .evalMap(_ => nodeContext.nodeState.get)
        .collect { case state: NodeState.Follower => state }
        .evalMap { state =>
          nodeContext.messageId.getAndUpdate(_.increment).map { id =>
            Request.RequestVote(
              id = id,
              term = state.currentTerm,
              candidateId = CandidateId(
                id = state.id,
                ip = "127.0.0.0",
                port = 6666,
              ),
              lastLogIndex = Index.zero,
              lastLogTerm = state.currentTerm,
            )
          }
        }
        .evalTap(r => IO.println(s"outbound: $r"))
        .map(request => s"${request.asJson.noSpaces}\n")
        .through(text.utf8.encode)
        .through(socket.writes)
        .interruptWhen(signal)
        .compile
        .drain

    val reads: IO[Unit] =
      socket.reads
        .through(text.utf8.decode)
        .through(text.lines)
        .evalTap(r => IO.println(s"inbound: $r"))
        .evalMap { line =>
          IO.fromEither(parser.parse(line))
        }
        .evalMap { json =>
          IO.fromEither(json.as[Response])
        }
        .collect { case v: Response.RequestVote => v }
        .interruptWhen(signal)
        .compile
        .drain

    Supervisor[IO].use { supervisor =>
      for {
        _ <- supervisor.supervise(writes)
        _ <- reads
      } yield ()
    }
  }

  private def socketStream(address: SocketAddress[Host], signal: Signal[IO, Boolean]): IO[Unit] =
    Stream
      .resource(Network[IO].client(address))
      .evalMap { socket =>
        pipe(signal, socket)
      }
      .compile
      .drain

}

object VoteStream {
  def instance(nodeContext: NodeContext): VoteStream =
    new VoteStream(nodeContext)
}

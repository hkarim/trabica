package trabica.service

import cats.effect.*
import cats.effect.std.{Queue, Supervisor}
import io.circe.*
import io.circe.syntax.*
import fs2.*
import fs2.concurrent.*
import fs2.io.net.Network
import com.comcast.ip4s.*

import scala.concurrent.duration.*
import trabica.context.NodeContext
import trabica.model.*

class VoteStream(nodeContext: NodeContext) {

  def run: IO[Unit] =
    Supervisor[IO].use { supervisor =>
      for {
        outbound <- Queue.unbounded[IO, Request.RequestVote]
        inbound  <- Queue.unbounded[IO, Response.RequestVote]
        signal   <- SignallingRef.of[IO, Boolean](false)
        _        <- supervisor.supervise(enqueueStream(signal, outbound))
        _        <- supervisor.supervise(dequeStream(signal, inbound))
        peerIp   <- host("trabica.peer.ip")
        peerPort <- port("trabica.peer.port")
        address = SocketAddress(peerIp, peerPort)
        socket <- socketStream(address, signal, outbound, inbound)
      } yield socket
    }

  private def host(key: String): IO[Host] =
    IO.fromOption(Host.fromString(nodeContext.config.getString(key)))(
      new IllegalArgumentException(s"invalid host value for `$key`")
    )

  private def port(key: String): IO[Port] =
    IO.fromOption(Port.fromInt(nodeContext.config.getInt(key)))(
      new IllegalArgumentException(s"invalid port value for `$key`")
    )

  private def enqueueStream(signal: Signal[IO, Boolean], outbound: Queue[IO, Request.RequestVote]): IO[Unit] =
    Stream
      .awakeEvery[IO](2.seconds)
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
      .evalMap(outbound.offer)
      .interruptWhen(signal)
      .compile
      .drain

  private def dequeStream(signal: Signal[IO, Boolean], inbound: Queue[IO, Response.RequestVote]): IO[Unit] =
    Stream
      .fromQueueUnterminated(inbound)
      .interruptWhen(signal)
      .compile
      .drain

  private def socketStream(
    address: SocketAddress[Host],
    signal: Signal[IO, Boolean],
    outbound: Queue[IO, Request.RequestVote],
    inbound: Queue[IO, Response.RequestVote],
  ): IO[Unit] =
    Stream.resource(Network[IO].client(address)).flatMap { socket =>
      val writes = Stream
        .eval(outbound.take)
        .map(request => s"${request.asJson}\n")
        .through(text.utf8.encode)
        .through(socket.writes)

      val reads = socket.reads
        .through(text.utf8.decode)
        .through(text.lines)
        .evalMap { line =>
          IO.fromEither(parser.parse(line))
        }
        .evalMap { json =>
          IO.fromEither(json.as[Response])
        }
        .collect { case v: Response.RequestVote => v }
        .evalMap(inbound.offer)

      (writes ++ reads).repeat.interruptWhen(signal)
    }.compile.drain

}

object VoteStream {
  def instance(nodeContext: NodeContext): VoteStream =
    new VoteStream(nodeContext)
}

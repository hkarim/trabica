package trabica.service

import cats.syntax.all.*
import cats.effect.*
import cats.effect.std.Supervisor
import com.comcast.ip4s.{Host, SocketAddress}
import fs2.Stream
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.{Network, Socket}
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.*

import scala.concurrent.duration.*

object JoinStream {

  def run(context: NodeContext, state: NodeState.Orphan, supervisor: Supervisor[IO]): IO[Unit] =
    state.peers.toVector.traverse { peer =>
      socketStream(context, SocketAddress(peer.ip, peer.port)).supervise(supervisor)
    }.void

  private def onJoinResponse(context: NodeContext, response: Response.Join): IO[Unit] =
    for {
      oldState <- context.nodeState.get
      newState = NodeState.NonVoter(
        id = oldState.id,
        self = oldState.self,
        peers = oldState.peers + response.header.peer,
        currentTerm = response.header.term,
        votedFor = None,
        commitIndex = Index.zero,
        lastApplied = Index.zero,
      )
      _ <- context.events.offer(Event.NodeStateEvent(newState))
    } yield ()

  private def communicate(context: NodeContext, socket: Socket[IO]): IO[Unit] = {
    val writes: IO[Unit] =
      Stream
        .awakeEvery[IO](2.seconds)
        .evalTap(_ => IO.println("[JoinStream] wake up"))
        .evalMap(_ => context.nodeState.get)
        .evalMap { state =>
          context.messageId.getAndUpdate(_.increment).map { id =>
            Request.Join(
              header = Header(
                peer = state.self,
                messageId = id,
                term = state.currentTerm,
              ),
            )
          }
        }
        .through(StreamEncoder.many(Encoder[Request]).toPipeByte)
        .through(socket.writes)
        .compile
        .drain

    val reads: IO[Unit] =
      socket.reads
        .through(StreamDecoder.many(Decoder[Response]).toPipeByte)
        .collect { case v: Response.Join => v }
        .evalTap(response => IO.println(s"[JoinStream] joined cluster through peer ${response.header.peer}"))
        .evalMap(response => onJoinResponse(context, response))
        .compile
        .drain

    Supervisor[IO].use { supervisor =>
      for {
        _ <- supervisor.supervise(writes)
        _ <- reads
      } yield ()
    }
  }

  private def socketStream(context: NodeContext, address: SocketAddress[Host]): IO[Unit] =
    Stream
      .resource(Network[IO].client(address))
      .interruptWhen(context.interrupt)
      .evalMap { socket =>
        communicate(context, socket)
      }
      .compile
      .drain

}

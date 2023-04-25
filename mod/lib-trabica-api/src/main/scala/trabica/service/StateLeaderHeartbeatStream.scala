package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import cats.syntax.all.*
import com.comcast.ip4s.{Host, SocketAddress}
import fs2.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.{Network, Socket}
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.*

import scala.concurrent.duration.*

object StateLeaderHeartbeatStream {

  private final val logger = scribe.cats[IO]

  def run(context: NodeContext, state: NodeState.Leader, supervisor: Supervisor[IO]): IO[Unit] =
    logger.info("starting up heartbeat stream") >>
      state.peers.toVector.traverse { peer =>
        socketStream(context, SocketAddress(peer.ip, peer.port)).supervise(supervisor)
      }.void

  private def onResponse(context: NodeContext, response: Response.AppendEntries): IO[Unit] =
    context.nodeState.get.flatMap { state =>
      if state.currentTerm < response.header.term then
        context.events.offer(
          Event.NodeStateChangedEvent(
            NodeState.Follower(
              id = state.id,
              self = state.self,
              peers = state.peers,
              leader = response.header.peer,
              currentTerm = response.header.term,
              votedFor = None,
              commitIndex = state.commitIndex,
              lastApplied = state.lastApplied,
            )
          )
        )
      else
        IO.unit
    }

  private def pipe(context: NodeContext, socket: Socket[IO]): IO[Unit] = {
    val writes: IO[Unit] =
      Stream
        .awakeEvery[IO](2.seconds)
        .evalTap(_ => logger.debug("wake up"))
        .evalMap(_ => context.nodeState.get)
        .evalMap { state =>
          context.messageId.getAndUpdate(_.increment).map { id =>
            Request.AppendEntries(
              header = Header(
                peer = state.self,
                messageId = id,
                term = state.currentTerm,
              ),
              prevLogIndex = state.lastApplied,
              prevLogTerm = state.currentTerm,
              entries = Vector.empty,
              leaderCommitIndex = state.lastApplied,
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
        .collect { case v: Response.AppendEntries => v }
        .evalMap(response => onResponse(context, response))
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
        pipe(context, socket)
      }
      .compile
      .drain

}

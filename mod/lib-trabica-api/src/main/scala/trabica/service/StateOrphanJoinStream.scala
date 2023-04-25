package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import cats.syntax.all.*
import com.comcast.ip4s.{Host, SocketAddress}
import fs2.Stream
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.{Network, Socket}
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.*
import trabica.model.Response.JoinStatus

import scala.concurrent.duration.*

object StateOrphanJoinStream {

  private final val logger = scribe.cats[IO]

  def run(context: NodeContext, state: NodeState.Orphan, supervisor: Supervisor[IO]): IO[Unit] =
    logger.info("starting up join stream") >>
      state.peers.toVector.traverse { peer =>
        socketStream(context, SocketAddress(peer.ip, peer.port)).supervise(supervisor)
      }.void

  private def onJoinResponse(context: NodeContext, response: Response.Join): IO[Unit] =
    for {
      oldState <- context.nodeState.get
      _ <- response.status match {
        case JoinStatus.Accepted =>
          val newState = NodeState.Follower(
            id = oldState.id,
            self = oldState.self,
            peers = oldState.peers + response.header.peer,
            leader = response.header.peer,
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
          )
          context.events.offer(Event.NodeStateChangedEvent(newState))
        case JoinStatus.UnknownLeader(knownPeers) =>
          val newState = NodeState.Orphan(
            id = oldState.id,
            self = oldState.self,
            peers = knownPeers.toSet,
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
          )
          context.events.offer(Event.NodeStateChangedEvent(newState))
        case JoinStatus.Forward(leader) =>
          val newState = NodeState.Orphan(
            id = oldState.id,
            self = oldState.self,
            peers = Set(leader),
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
          )
          context.events.offer(Event.NodeStateChangedEvent(newState))
      }

      _ <- logger.info(s"joining cluster through peer ${response.header.peer}")
    } yield ()

  private def communicate(context: NodeContext, socket: Socket[IO]): IO[Unit] = {
    val writes: IO[Unit] =
      Stream
        .awakeEvery[IO](2.seconds)
        .evalTap(_ => logger.debug("wake up"))
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

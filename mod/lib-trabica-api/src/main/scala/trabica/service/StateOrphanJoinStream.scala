package trabica.service

import cats.effect.*
import cats.syntax.all.*
import com.comcast.ip4s.SocketAddress
import fs2.Stream
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.*
import trabica.model.Response.JoinStatus

import scala.concurrent.duration.*

class StateOrphanJoinStream(context: NodeContext, id: Int) {

  private final val logger = scribe.cats[IO]

  def run: IO[Unit] =
    context.nodeState.get.flatMap {
      case state: NodeState.Orphan =>
        logger.info(s"orphan [$id] restarting heartbeat stream") >>
          state.peers.toVector.traverse { peer =>
            socketStream(peer)
          }
      case state =>
        IO.raiseError(NodeError.InvalidNodeState(state))
    }
      .void
      .onCancel {
        logger.debug(s"orphan [$id] all join streams canceled")
      }

  private def onJoinResponse(response: Response.Join): IO[Unit] =
    for {
      _        <- logger.info(s"orphan [$id] join stream received response: $response")
      oldState <- context.nodeState.get
      _ <- response.status match {
        case JoinStatus.Accepted =>
          def newState(signal: Deferred[IO, Unit]) = NodeState.Follower(
            id = oldState.id,
            self = oldState.self,
            peers = oldState.peers + response.header.peer,
            leader = response.header.peer,
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
            signal = signal,
          )
          for {
            _      <- logger.info(s"orphan [$id] joining cluster response `accepted` through peer ${response.header.peer}")
            signal <- Deferred[IO, Unit]
            _      <- OperationService.stateChanged(context, newState(signal))
          } yield ()

        case JoinStatus.UnknownLeader(knownPeers) =>
          def newState(signal: Deferred[IO, Unit]) = NodeState.Orphan(
            id = oldState.id,
            self = oldState.self,
            peers = knownPeers.toSet,
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
            signal = signal,
          )
          for {
            _ <- logger.info(s"orphan [$id] joining cluster response `unknown-leader`, using known peers of peer ${response.header.peer}")
            signal <- Deferred[IO, Unit]
            _      <- OperationService.stateChanged(context, newState(signal))
          } yield ()
        case JoinStatus.Forward(leader) =>
          def newState(signal: Deferred[IO, Unit]) = NodeState.Orphan(
            id = oldState.id,
            self = oldState.self,
            peers = Set(leader),
            currentTerm = response.header.term,
            votedFor = None,
            commitIndex = Index.zero,
            lastApplied = Index.zero,
            signal = signal,
          )
          for {
            _      <- logger.info(s"orphan [$id] joining cluster response `forward`, through peer ${response.header.peer}")
            signal <- Deferred[IO, Unit]
            _      <- OperationService.stateChanged(context, newState(signal))
          } yield ()
      }
    } yield ()

  private def socketStream(peer: Peer): IO[Unit] = {
    val address = SocketAddress(peer.ip, peer.port)

    val communication =
      Stream
        .fixedRateStartImmediately[IO](2.seconds)
        .evalTap(_ => logger.debug(s"orphan [$id] join wake up"))
        .evalMap(_ => context.nodeState.get)
        .flatMap { state =>
          Stream
            .resource(Network[IO].client(address))
            .flatMap { socket =>
              Stream
                .eval {
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
                .evalTap(_ => logger.debug(s"orphan [$id] sending join request to $peer"))
                .through(StreamEncoder.once(Encoder[Request]).toPipeByte)
                .through(socket.writes)
                .evalMap { _ =>
                  socket.reads
                    .through(StreamDecoder.once(Decoder[Response]).toPipeByte)
                    .collect { case v: Response.Join => v }
                    .evalTap(_ => logger.debug(s"orphan [$id] join response from $peer"))
                    .evalMap(onJoinResponse)
                    .head
                    .compile
                    .drain
                }
            }
        }
        .compile
        .drain

    Resilient
      .retry(communication, 1, 4)

  }

}

object StateOrphanJoinStream {
  def instance(context: NodeContext, id: Int): StateOrphanJoinStream =
    new StateOrphanJoinStream(context, id)
}

package trabica.service

import cats.effect.*
import cats.syntax.all.*
import com.comcast.ip4s.SocketAddress
import fs2.*
import fs2.io.net.Network
import scodec.bits.ByteVector
import scodec.{Attempt, DecodeResult, Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.*

import scala.concurrent.duration.*

class StateLeaderHeartbeatStream(context: NodeContext, id: Int) {

  private final val logger = scribe.cats[IO]

  def run: IO[Unit] =
    context.nodeState.get.flatMap {
      case state: NodeState.Leader =>
        logger.info(s"leader [$id] restarting heartbeat stream") >>
          state.peers.toVector.traverse { peer =>
            socketStream(state, peer)
          }
      case state =>
        IO.raiseError(NodeError.InvalidNodeState(state))
    }
      .void
      .onCancel {
        logger.debug(s"leader [$id] all heartbeat streams canceled")
      }

  private def onResponse(response: Response.AppendEntries): IO[Unit] =
    for {
      state <- context.nodeState.get
      _ <-
        if state.currentTerm < response.header.term then {
          def newState(signal: Deferred[IO, Unit]) =
            NodeState.Follower(
              id = state.id,
              self = state.self,
              peers = state.peers,
              leader = response.header.peer,
              currentTerm = response.header.term,
              votedFor = None,
              commitIndex = state.commitIndex,
              lastApplied = state.lastApplied,
              signal = signal,
            )

          for {
            _      <- logger.debug(s"leader [$id] changing state due discovered higher term")
            _      <- logger.debug(s"leader [$id] current term ${state.currentTerm}, higher term ${response.header.term}")
            signal <- Deferred[IO, Unit]
            _      <- OperationService.stateChanged(context, newState(signal))
          } yield ()
        } else IO.unit

    } yield ()

  private def socketStream(state: NodeState.Leader, peer: Peer): IO[Unit] = {
    val address = SocketAddress(peer.ip, peer.port)

    val communication = for {
      _         <- logger.debug(s"leader [$id] heartbeat wake up")
      state     <- context.nodeState.get
      messageId <- context.messageId.getAndUpdate(_.increment)
      request = Request.AppendEntries(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        prevLogIndex = state.lastApplied,
        prevLogTerm = state.currentTerm,
        entries = Vector.empty,
        leaderCommitIndex = state.lastApplied,
      )
      bytes <- Encoder[Request].encode(request).map(_.bytes) match {
        case Attempt.Successful(value) =>
          IO.pure(value)
        case Attempt.Failure(cause) =>
          IO.raiseError(new IllegalArgumentException(cause.toString))
      }
      _ <- Network[IO].client(address).use[Unit] { socket =>
        for {
          _ <- socket.write(Chunk.byteVector(bytes))
          _ <- socket.endOfOutput
          o <- socket.read(1024).timeout(100.milliseconds)
          _ <- socket.endOfInput
          r <- o match {
            case Some(value) =>
              Decoder[Response].decode(ByteVector.apply(value.toVector).bits) match {
                case Attempt.Successful(DecodeResult(v: Response.AppendEntries, _)) =>
                  onResponse(v)
                case _ =>
                  IO.unit
              }
            case None =>
              IO.unit
          }
        } yield r
      }

    } yield ()

    val stream =
      Stream
        .fixedRateStartImmediately[IO](2.seconds)
        .evalTap(_ => communication)
        .compile
        .drain

    Resilient
      .retry(stream, 1, 4)
      .handleErrorWith { e =>
        for {
          _           <- logger.debug(s"exhausted retries for peer $peer due to ${e.getMessage}")
          stateSignal <- Deferred[IO, Unit]
          _           <- logger.debug(s"will remove peer $peer from known peers")
          newState = state.copy(peers = state.peers - peer, signal = stateSignal)
          _ <- OperationService.stateChanged(context, newState)
        } yield ()
      }

  }

}

object StateLeaderHeartbeatStream {
  def instance(context: NodeContext, id: Int): StateLeaderHeartbeatStream =
    new StateLeaderHeartbeatStream(context, id)
}

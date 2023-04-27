package trabica.fsm

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import trabica.context.NodeContext
import trabica.model.NodeState
import trabica.net.GrpcClient
import trabica.rpc.JoinResponse.Status
import trabica.rpc.{Header, JoinRequest, JoinResponse, TrabicaFs2Grpc}

import scala.concurrent.duration.*

class OrphanNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState],
  val events: Queue[IO, NodeState],
  val signal: Deferred[IO, Unit],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  override final val server: TrabicaFs2Grpc[IO, Metadata] =
    StateOrphanService.instance(context, state, events)

  override def interrupt: IO[Unit] =
    signal.complete(()).void

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(peer)
      }
    } yield clients

  private def joinStream: IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .flatMap { _ =>
        Stream
          .resource(clients)
          .evalMap { cs =>
            for {
              messageId <- context.messageId.getAndUpdate(_.increment)
              s         <- state.get
              request = JoinRequest(
                header = Header(
                  peer = s.self.some,
                  messageId = messageId.value,
                  term = s.currentTerm,
                ).some
              )
              responses <- cs.parTraverse { c =>
                c.join(request, new Metadata)
                  .timeout(100.milliseconds)
                  .attempt
                  .flatMap(onResponse)
              }
            } yield responses
          }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"[orphan-${trace.orphanId}] error encountered in join stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def onResponse(response: Either[Throwable, JoinResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"[orphan-${trace.orphanId}] no response ${e.getMessage}")
      case Right(r) =>
        r.status match {
          case Status.Empty =>
            IO.unit
          case Status.Accepted(_) =>
            for {
              _ <- logger.debug(s"[orphan-${trace.orphanId}] response: $r")
              currentState <- state.orphan
              header <- r.header.required
              peer <- header.peer.required
              newState = NodeState.Follower(
                id = currentState.id,
                self = currentState.self,
                peers = Set(peer),
                leader = peer,
                currentTerm = header.term,
                votedFor = None,
                commitIndex = currentState.commitIndex,
                lastApplied = currentState.lastApplied,
              )
              _ <- events.offer(newState)
            } yield ()
          case Status.Forward(JoinResponse.Forward(leaderOption, _)) =>
            for {
              _ <- logger.debug(s"[orphan-${trace.orphanId}] response: $r")
              leader <- leaderOption.required
              currentState <- state.orphan
              newState = currentState.copy(peers = Set(leader))
              _ <- state.set(newState)
            } yield ()
          case Status.UnknownLeader(JoinResponse.UnknownLeader(knownPeers, _)) =>
            for {
              _ <- logger.debug(s"[orphan-${trace.orphanId}] response: $r")
              currentState <- state.orphan
              newState = currentState.copy(peers = currentState.peers ++ Set.from(knownPeers))
              _ <- state.set(newState)
            } yield ()
        }
    }

  def run: IO[Unit] = joinStream

}

object OrphanNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState],
    events: Queue[IO, NodeState],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): OrphanNode =
    new OrphanNode(
      context,
      state,
      events,
      signal,
      supervisor,
      trace,
    )
}

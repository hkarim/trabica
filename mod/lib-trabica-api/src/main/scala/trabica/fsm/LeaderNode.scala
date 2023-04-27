package trabica.fsm

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import trabica.context.NodeContext
import trabica.model.NodeState
import trabica.net.GrpcClient
import trabica.rpc.*

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState],
  val events: Queue[IO, NodeState],
  val signal: Deferred[IO, Unit],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  override final val server: TrabicaFs2Grpc[IO, Metadata] =
    StateLeaderService.instance(context, state, events)

  override def interrupt: IO[Unit] =
    signal.complete(()).void

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(peer)
      }
    } yield clients

  private def heartbeatStream: IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .flatMap { _ =>
        Stream
          .resource(clients)
          .evalMap { cs =>
            for {
              messageId <- context.messageId.getAndUpdate(_.increment)
              s         <- state.get
              request = AppendEntriesRequest(
                header = Header(
                  peer = s.self.some,
                  messageId = messageId.value,
                  term = s.currentTerm,
                ).some
              )
              responses <- cs.parTraverse { c =>
                c.appendEntries(request, new Metadata)
                  .timeout(100.milliseconds)
                  .attempt
                  .flatMap(onResponse)
              }
            } yield responses
          }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"[leader-${trace.leaderId}] error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def onResponse(response: Either[Throwable, AppendEntriesResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"[leader-${trace.leaderId}] no response ${e.getMessage}")
      case Right(r) =>
        for {
          header       <- r.header.required
          currentState <- state.get
          _ <-
            if header.term > currentState.currentTerm then {
              for {
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
            } else
              IO.unit
        } yield ()
    }

  def run: IO[Unit] = heartbeatStream

}

object LeaderNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState],
    events: Queue[IO, NodeState],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): LeaderNode =
    new LeaderNode(
      context,
      state,
      events,
      signal,
      supervisor,
      trace,
    )
}

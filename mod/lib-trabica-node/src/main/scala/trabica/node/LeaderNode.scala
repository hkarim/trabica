package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.*
import trabica.net.NodeApi

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val quorumId: String,
  val quorumPeer: Peer,
  val state: Ref[IO, NodeState.Leader],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val streamSignal: SignallingRef[IO, Boolean],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node[NodeState.Leader] {

  override final val logger = scribe.cats[IO]

  private final val id: Int = trace.leaderId

  override final val prefix: String = s"[leader-$id]"

  private final val heartbeatStreamRate: Long =
    context.config.getLong("trabica.leader.heartbeat-stream.rate")

  override def lens: NodeStateLens[NodeState.Leader] =
    NodeStateLens[NodeState.Leader]

  override def run: IO[FiberIO[Unit]] =
    clients.use(heartbeatStream).supervise(supervisor)

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >>
      signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  private def heartbeatStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream(clients)
      .interruptWhen(streamSignal)
      .filter(_.nonEmpty)
      .evalTap(_ => logger.debug(s"$prefix starting heartbeat stream"))
      .flatMap { cs =>
        Stream
          .fixedRateStartImmediately[IO](heartbeatStreamRate.milliseconds)
          .interruptWhen(streamSignal)
          .evalTap(_ => logger.trace(s"$prefix heartbeat stream wake up"))
          .evalMap { _ =>
            for {
              currentState <- state.get
              h            <- makeHeader(currentState)
              request = AppendEntriesRequest(
                header = h.some,
                prevLogIndex = currentState.commitIndex.value - 1,
                prevLogTerm = currentState.localState.currentTerm - 1,
              )
              responses <- cs.parTraverse { c =>
                logger.trace(s"$prefix sending heartbeat to ${c.quorumPeer.host}:${c.quorumPeer.port}") >>
                  c.appendEntries(request)
                    .timeout(100.milliseconds)
                    .attempt
                    .flatMap(r => onResponse(c.quorumPeer, r))
              }
            } yield responses
          }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .onFinalize {
        logger.debug(s"$prefix heartbeat stream finalized")
      }
      .compile
      .drain

  private def onResponse(peer: Peer, response: Either[Throwable, AppendEntriesResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.trace(s"$prefix no response from peer ${peer.host}:${peer.port}, error: ${e.getMessage}")
      case Right(r) =>
        logger.trace(s"$prefix response `${r.success}` from peer ${peer.host}:${peer.port}")
    }

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required(NodeError.InvalidMessage)
      _            <- Node.termCheck(header, currentState, events)
    } yield false

}

object LeaderNode {
  def instance(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    state: Ref[IO, NodeState.Leader],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[LeaderNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new LeaderNode(
      context,
      quorumId,
      quorumPeer,
      state,
      events,
      signal,
      streamSignal,
      supervisor,
      trace,
    )
  } yield node
}

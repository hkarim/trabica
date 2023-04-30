package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Leader],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.leaderId

  private final val prefix: String = s"[leader-$id]"

  private final val heartbeatStreamRate: Long =
    context.config.getLong("trabica.leader.heartbeat-stream.rate")

  override def interrupt: IO[Unit] =
    signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def clients: Resource[IO, Vector[NodeApi]] =
    for {
      currentState <- Resource.eval(state.get)
      clients <- currentState.peers.toVector.traverse { peer =>
        NodeApi.client(prefix, peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Leader): IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix peers changed, restarting leader")
      currentState <- state.get
      _ <- events.offer(
        Event.NodeStateChanged(currentState, newState, reason = StateTransitionReason.ConfigurationChanged)
      )
    } yield ()

  private def heartbeatStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream(clients)
      .interruptWhen(signal)
      .filter(_.nonEmpty)
      .evalTap(_ => logger.debug(s"$prefix starting heartbeat stream"))
      .flatMap { cs =>
        Stream
          .fixedRateStartImmediately[IO](heartbeatStreamRate.milliseconds)
          .interruptWhen(signal)
          .evalTap(_ => logger.debug(s"$prefix heartbeat stream wake up"))
          .evalMap { _ =>
            for {
              currentState <- state.get
              h            <- header(currentState)
              request = AppendEntriesRequest(
                header = h.some,
                peers = currentState.peers.toSeq,
              )
              responses <- cs.parTraverse { c =>
                c.appendEntries(request)
                  .timeout(100.milliseconds)
                  .attempt
                  .flatMap(r => onResponse(c.peer, r))
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
        logger.debug(s"$prefix no response from peer ${peer.host}:${peer.port}, error: ${e.getMessage}")
      case Right(r) =>
        for {
          header       <- r.header.required
          currentState <- state.get
          _            <- Node.termCheck(header, currentState, events)
        } yield ()
    }

  def run: IO[FiberIO[Unit]] =
    clients.use(heartbeatStream).supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      _            <- Node.termCheck(header, currentState, events)
    } yield false

  override def join(request: JoinRequest): IO[JoinResponse.Status] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} joining")
      currentState <- state.get
      status = JoinResponse.Status.Accepted(
        JoinResponse.Accepted()
      )
      _ <-
        if !currentState.peers.contains(peer) then {
          val newState = currentState.copy(peers = currentState.peers + peer)
          peersChanged(newState)
        } else IO.unit
    } yield status

  override def vote(request: VoteRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      _ <- logger.debug(
        s"$prefix vote requested. votedFor=${currentState.votedFor}, term=${currentState.currentTerm}, request.term=${header.term}"
      )
      voteGranted = currentState.votedFor.isEmpty && Term.of(header.term) > currentState.currentTerm
      _ <-
        if voteGranted then {
          for {
            peer <- header.peer.required
            _    <- state.set(currentState.copy(votedFor = peer.some))
          } yield ()
        } else IO.unit
    } yield voteGranted

}

object LeaderNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Leader],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[LeaderNode] = IO.pure {
    new LeaderNode(
      context,
      state,
      events,
      signal,
      supervisor,
      trace,
    )
  }
}

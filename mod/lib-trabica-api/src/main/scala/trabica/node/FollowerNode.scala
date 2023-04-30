package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

class FollowerNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Follower],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val heartbeat: Ref[IO, Option[Unit]],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.followerId

  private final val prefix: String = s"[follower-$id]"

  private final val heartbeatStreamTimeout: Long =
    context.config.getLong("trabica.follower.heartbeat-stream.timeout")

  override def interrupt: IO[Unit] =
    signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def heartbeatStream: IO[Unit] =
    Stream
      .fixedRate[IO](heartbeatStreamTimeout.milliseconds)
      .interruptWhen(signal)
      .evalTap(_ => logger.debug(s"$prefix heartbeat wake up"))
      .evalMap { _ =>
        heartbeat.flatModify[Unit] {
          case Some(_) =>
            (None, logger.debug(s"$prefix heartbeat good to go"))
          case None =>
            (
              None,
              logger.debug(s"$prefix heartbeat no elements, will timeout") >>
                timeout
            )
        }
      }
      .handleErrorWith { e =>
        Stream.eval {
          e match {
            case _: TimeoutException =>
              timeout
            case e =>
              logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e) >>
                IO.raiseError(e)
          }
        }
      }
      .onFinalize {
        logger.debug(s"$prefix heartbeat stream finalized")
      }
      .compile
      .drain

  private def timeout: IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix heartbeat stream timed out")
      currentState <- state.get
      _            <- logger.debug(s"$prefix switching to candidate, ${currentState.peers.size} peer(s) known")
      newState = NodeState.Candidate(
        self = currentState.self,
        peers = currentState.peers,
        currentTerm = currentState.currentTerm.increment,
        votedFor = currentState.self.some, // vote for self
        commitIndex = currentState.commitIndex,
        lastApplied = currentState.lastApplied,
        votes = Set(currentState.self),
      )
      - <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.NoHeartbeat))
    } yield ()

  def run: IO[FiberIO[Unit]] =
    heartbeatStream.supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      _            <- heartbeat.set(Some(()))
      currentState <- state.get
      header       <- request.header.required
      peer         <- header.peer.required
      _            <- logger.debug(s"$prefix updating peers, ${request.peers.length} peer(s) known to leader")
      _ <- state.set(
        currentState.copy(peers = request.peers.toSet + peer - currentState.self)
      ) // copy the current peers from leader including the leader and excluding self
      _ <- Node.termCheck(header, currentState, events)
    } yield false

  override def join(request: JoinRequest): IO[JoinResponse.Status] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} joining")
      currentState <- state.get
      status = JoinResponse.Status.Forward(
        JoinResponse.Forward(leader = currentState.leader.some)
      )
    } yield status

  override def vote(request: VoteRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
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

object FollowerNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Follower],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[FollowerNode] = for {
    heartbeat <- Ref.of[IO, Option[Unit]](None)
    node = new FollowerNode(
      context,
      state,
      events,
      signal,
      heartbeat,
      supervisor,
      trace,
    )
  } yield node
}

package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.{Event, NodeState}
import trabica.rpc.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

class FollowerNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Follower],
  val events: Queue[IO, Event],
  val signal: Deferred[IO, Unit],
  val streamSignal: SignallingRef[IO, Boolean],
  val heartbeatQueue: Queue[IO, Unit],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.followerId

  private final val prefix: String = s"[follower-$id]"

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def heartbeatStream: IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.debug(s"$prefix heartbeat wake up"))
      .evalMap(_ => heartbeatQueue.take.timeout(3.seconds).attempt)
      .evalMap {
        case Left(_: TimeoutException) =>
          timeout
        case Left(e) =>
          IO.raiseError(e)
        case Right(_) =>
          IO.unit
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def timeout: IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix heartbeat stream timed out")
      currentState <- state.get
      _            <- logger.debug(s"$prefix switching to candidate, ${currentState.peers.size} peer(s) known")
      newState = NodeState.Candidate(
        id = currentState.id,
        self = currentState.self,
        peers = currentState.peers,
        currentTerm = currentState.currentTerm + 1,
        votedFor = currentState.self.some, // vote for self
        commitIndex = currentState.commitIndex,
        lastApplied = currentState.lastApplied
      )
      - <- events.offer(Event.NodeStateChanged(newState))
    } yield ()

  def run: IO[FiberIO[Unit]] =
    heartbeatStream.supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      peer         <- header.peer.required
      _            <- heartbeatQueue.offer(())
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
      voteGranted = currentState.votedFor.isEmpty && header.term > currentState.currentTerm
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
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[FollowerNode] = for {
    streamSignal   <- SignallingRef.of[IO, Boolean](false)
    heartbeatQueue <- Queue.unbounded[IO, Unit]
    node = new FollowerNode(
      context,
      state,
      events,
      signal,
      streamSignal,
      heartbeatQueue,
      supervisor,
      trace,
    )
  } yield node
}

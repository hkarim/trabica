package trabica.fsm

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.context.NodeContext
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

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"[follower-$id] interrupted")

  private def heartbeatStream: IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.debug(s"[follower-$id] heartbeat wake up"))
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
          logger.error(s"[follower-$id] error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def timeout: IO[Unit] =
    for {
      _            <- logger.debug(s"[follower-$id] heartbeat stream timed out")
      currentState <- state.get
      _            <- logger.debug(s"[follower-$id] switching to candidate, ${currentState.peers.size} known")
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

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = AppendEntriesResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
      )
      header <- request.header.required
      _      <- heartbeatQueue.offer(())
      _      <- logger.debug(s"[follower-$id] updating peers, ${request.peers.length} peer(s) known to leader")
      _ <- state.set(currentState.copy(peers = request.peers.toSet - currentState.self)) // copy the current peers from leader
      _ <- Node.termCheck(header, currentState, events)
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"[follower-$id] peer ${peer.host}:${peer.port} joining")
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = JoinResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
        status = JoinResponse.Status.Forward(
          JoinResponse.Forward(leader = currentState.leader.some)
        )
      )
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      header       <- request.header.required
      voteGranted = currentState.votedFor.isEmpty && header.term > currentState.currentTerm
      response = VoteResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
        voteGranted = voteGranted
      )
      _ <-
        if voteGranted then {
          for {
            peer <- header.peer.required
            _    <- state.set(currentState.copy(votedFor = peer.some))
          } yield ()
        } else IO.unit
    } yield response

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

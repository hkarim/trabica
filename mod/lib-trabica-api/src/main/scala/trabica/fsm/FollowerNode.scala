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

import scala.concurrent.duration.*

class FollowerNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Follower],
  val events: Queue[IO, Event],
  val signal: Deferred[IO, Unit],
  val streamSignal: SignallingRef[IO, Boolean],
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
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"[follower-$id] error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

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
      _      <- termCheck(header, currentState)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = VoteResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
      )
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

  private def termCheck(header: Header, currentState: NodeState): IO[Unit] =
    for {
      peer <- header.peer.required
      _ <-
        if header.term > currentState.currentTerm then {
          val newState = NodeState.Follower(
            id = currentState.id,
            self = currentState.self,
            peers = Set(peer),
            leader = peer,
            currentTerm = header.term,
            votedFor = None,
            commitIndex = currentState.commitIndex,
            lastApplied = currentState.lastApplied,
          )
          events.offer(Event.NodeStateChanged(newState))
        } else IO.unit

    } yield ()

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
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new FollowerNode(
      context,
      state,
      events,
      signal,
      streamSignal,
      supervisor,
      trace,
    )
  } yield node
}

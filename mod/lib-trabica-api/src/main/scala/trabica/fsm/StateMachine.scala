package trabica.fsm

import cats.effect.*
import cats.effect.std.{Mutex, Queue, Supervisor}
import io.grpc.Metadata
import fs2.*
import trabica.context.NodeContext
import trabica.model.{Event, NodeState}
import trabica.rpc.*

class StateMachine(
  val context: NodeContext,
  val node: Ref[IO, Node],
  val events: Queue[IO, Event],
  val supervisor: Supervisor[IO],
  val mutex: Mutex[IO],
  val trace: Ref[IO, NodeTrace],
) extends TrabicaFs2Grpc[IO, Metadata] {

  private final val logger = scribe.cats[IO]

  def run: IO[Unit] =
    eventStream

  private def transition(newState: NodeState): IO[FiberIO[Unit]] =
    mutex.lock.surround {
      for {
        currentNode <- node.get
        f <- newState match {
          case state: NodeState.Orphan =>
            logger.debug("transitioning to orphan") >>
              orphan(currentNode, state).supervise(supervisor)
          case state: NodeState.NonVoter => ???
          case state: NodeState.Follower =>
            logger.debug("transitioning to follower") >>
              follower(currentNode, state).supervise(supervisor)
          case state: NodeState.Candidate => ???
          case state: NodeState.Leader =>
            logger.debug("transitioning to leader") >>
              leader(currentNode, state).supervise(supervisor)
          case state: NodeState.Joint => ???
        }
      } yield f
    }

  private def eventStream: IO[Unit] =
    Stream
      .fromQueueUnterminated(events)
      .evalMap {
        case Event.NodeStateChanged(newState) =>
          transition(newState)
      }
      .compile
      .drain

  private def orphan(currentNode: Node, newState: NodeState.Orphan): IO[Unit] =
    for {
      signal  <- Deferred[IO, Unit]
      ref     <- Ref.of[IO, NodeState.Orphan](newState)
      t       <- trace.incrementOrphan
      _       <- logger.debug(s"[orphan-${t.orphanId}] starting node transition")
      newNode <- OrphanNode.instance(context, ref, events, signal, supervisor, t)
      _       <- logger.debug(s"[orphan-${t.orphanId}] interrupting current node")
      _       <- currentNode.interrupt
      _       <- node.set(newNode)
      f       <- newNode.run
      _       <- logger.debug(s"[orphan-${t.orphanId}] running, awaiting terminate signal")
      _       <- signal.get
      _       <- f.cancel
      _       <- logger.debug(s"[orphan-${t.orphanId}] terminated")
    } yield ()

  private def follower(currentNode: Node, newState: NodeState.Follower): IO[Unit] =
    for {
      signal  <- Deferred[IO, Unit]
      ref     <- Ref.of[IO, NodeState.Follower](newState)
      t       <- trace.incrementFollower
      _       <- logger.debug(s"[follower-${t.followerId}] starting node transition")
      newNode <- FollowerNode.instance(context, ref, events, signal, supervisor, t)
      _       <- logger.debug(s"[follower-${t.followerId}] interrupting current node")
      _       <- currentNode.interrupt
      _       <- node.set(newNode)
      f       <- newNode.run
      _       <- logger.debug(s"[follower-${t.followerId}] running, awaiting terminate signal")
      _       <- signal.get
      _       <- f.cancel
      _       <- logger.debug(s"[follower-${t.followerId}] terminated")
    } yield ()

  private def leader(currentNode: Node, newState: NodeState.Leader): IO[Unit] =
    for {
      signal  <- Deferred[IO, Unit]
      ref     <- Ref.of[IO, NodeState.Leader](newState)
      t       <- trace.incrementLeader
      _       <- logger.debug(s"[leader-${t.leaderId}] starting node transition")
      newNode <- LeaderNode.instance(context, ref, events, signal, supervisor, t)
      _       <- logger.debug(s"[leader-${t.leaderId}] interrupting current node")
      _       <- currentNode.interrupt
      _       <- node.set(newNode)
      f       <- newNode.run
      _       <- logger.debug(s"[leader-${t.leaderId}] running, awaiting terminate signal")
      _       <- signal.get
      _       <- f.cancel
      _       <- logger.debug(s"[leader-${t.leaderId}] terminated")
    } yield ()

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      server   <- node.get
      response <- server.appendEntries(request, metadata)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      server   <- node.get
      response <- server.vote(request, metadata)
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      server   <- node.get
      response <- server.join(request, metadata)
    } yield response
}

object StateMachine {

  def instance(context: NodeContext, node: Ref[IO, Node]): IO[StateMachine] =
    Supervisor[IO].use { supervisor =>
      for {
        trace  <- Ref.of[IO, NodeTrace](NodeTrace.instance)
        events <- Queue.unbounded[IO, Event]
        mutex  <- Mutex[IO]
        fsm = new StateMachine(context, node, events, supervisor, mutex, trace)
      } yield fsm
    }

}

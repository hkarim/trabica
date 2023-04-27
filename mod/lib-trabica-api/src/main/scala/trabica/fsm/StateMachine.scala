package trabica.fsm

import cats.effect.*
import cats.effect.std.{Queue, Supervisor}
import io.grpc.Metadata
import fs2.*
import trabica.model.NodeState
import trabica.rpc.*

class StateMachine(val node: Ref[IO, Node], val events: Queue[IO, NodeState], val trace: Ref[IO, NodeTrace])
  extends TrabicaFs2Grpc[IO, Metadata] {

  private final val logger = scribe.cats[IO]

  def run: IO[Unit] =
    eventStream

  private def transition(newState: NodeState): IO[FiberIO[Unit]] =
    Supervisor[IO].use { supervisor =>
      for {
        currentNode <- node.get
        f <- newState match {
          case state: NodeState.Orphan =>
            logger.debug("transitioning to orphan") >>
              orphan(currentNode, state, supervisor)
                .supervise(supervisor)
          case state: NodeState.NonVoter  => ???
          case state: NodeState.Follower  => ???
          case state: NodeState.Candidate => ???
          case state: NodeState.Leader =>
            logger.debug("transitioning to leader") >>
              leader(currentNode, state, supervisor)
                .supervise(supervisor)
          case state: NodeState.Joint => ???
        }
      } yield f
    }

  private def eventStream: IO[Unit] =
    Stream
      .fromQueueUnterminated(events)
      .evalMap(transition)
      .compile
      .drain

  private def orphan(currentNode: Node, newState: NodeState, supervisor: Supervisor[IO]): IO[Unit] =
    for {
      signal <- Deferred[IO, Unit]
      ref    <- Ref.of[IO, NodeState](newState)
      t      <- trace.incrementOrphan
      newNode = OrphanNode.instance(currentNode.context, ref, events, signal, supervisor, t)
      _ <- currentNode.interrupt
      _ <- node.set(newNode)
      _ <- newNode.run.supervise(supervisor)
      _ <- logger.debug(s"[orphan-${t.orphanId}] running, awaiting terminate signal")
      _ <- signal.get
      _ <- logger.debug(s"[orphan-${t.orphanId}] terminated")
    } yield ()

  private def leader(currentNode: Node, newState: NodeState, supervisor: Supervisor[IO]): IO[Unit] =
    for {
      signal <- Deferred[IO, Unit]
      ref    <- Ref.of[IO, NodeState](newState)
      t      <- trace.incrementLeader
      newNode = LeaderNode.instance(currentNode.context, ref, events, signal, supervisor, t)
      _ <- currentNode.interrupt
      _ <- node.set(newNode)
      _ <- newNode.run.supervise(supervisor)
      _ <- logger.debug(s"[leader-${t.leaderId}] running, awaiting terminate signal")
      _ <- signal.get
      _ <- logger.debug(s"[leader-${t.leaderId}] terminated")
    } yield ()

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      server   <- node.get.map(_.server)
      response <- server.appendEntries(request, metadata)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      server   <- node.get.map(_.server)
      response <- server.vote(request, metadata)
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      server   <- node.get.map(_.server)
      response <- server.join(request, metadata)
    } yield response
}

object StateMachine {

  def instance(node: Ref[IO, Node], events: Queue[IO, NodeState]): IO[StateMachine] =
    for {
      trace <- Ref.of[IO, NodeTrace](
        NodeTrace(orphanId = 0, leaderId = 0)
      )
      fsm = new StateMachine(node, events, trace)
    } yield fsm

}

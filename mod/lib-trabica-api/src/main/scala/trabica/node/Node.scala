package trabica.node

import cats.effect.*
import cats.effect.std.{Queue, UUIDGen}
import io.grpc.Metadata
import trabica.model.*
import trabica.rpc.*

trait Node extends TrabicaFs2Grpc[IO, Metadata] {

  def run: IO[FiberIO[Unit]]

  def interrupt: IO[Unit]

}

object Node {

  private final val logger = scribe.cats[IO]

  object DeadNode extends Node {
    override val run: IO[FiberIO[Unit]] =
      IO.raiseError(NodeError.Uninitialized)
    override val interrupt: IO[Unit] =
      IO.unit
    override def appendEntries(request: AppendEntriesRequest, ctx: Metadata): IO[AppendEntriesResponse] =
      IO.raiseError(NodeError.Uninitialized)
    override def vote(request: VoteRequest, ctx: Metadata): IO[VoteResponse] =
      IO.raiseError(NodeError.Uninitialized)
    override def join(request: JoinRequest, ctx: Metadata): IO[JoinResponse] =
      IO.raiseError(NodeError.Uninitialized)
  }

  def termCheck(header: Header, currentState: NodeState, events: Queue[IO, Event]): IO[Unit] =
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

  def state(command: CliCommand): IO[NodeState] = command match {
    case v: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(v)
    case v: CliCommand.Join =>
      logger.debug("initiating node state in orphan mode") >>
        orphan(v)
  }

  private def bootstrap(command: CliCommand.Bootstrap): IO[NodeState] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    state = NodeState.Leader(
      id = id,
      self = Peer(
        host = command.host,
        port = command.port,
      ),
      peers = Set.empty,
      votedFor = None,
      currentTerm = 0L,
      commitIndex = 0L,
      lastApplied = 0L,
      nextIndex = 0L,
      matchIndex = 0L,
    )
  } yield state

  private def orphan(command: CliCommand.Join): IO[NodeState] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    state = NodeState.Orphan(
      id = id,
      self = Peer(
        host = command.host,
        port = command.port,
      ),
      peers = Set(
        Peer(
          host = command.peerHost,
          port = command.peerPort,
        )
      ),
      currentTerm = 0L,
      votedFor = None,
      commitIndex = 0L,
      lastApplied = 0L,
    )
  } yield state

}

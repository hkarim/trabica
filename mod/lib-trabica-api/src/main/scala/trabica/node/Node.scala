package trabica.node

import cats.effect.*
import cats.effect.std.{Queue, Supervisor, UUIDGen}
import cats.syntax.all.*
import trabica.model.*
import trabica.rpc.*

trait Node {

  def context: NodeContext

  def stateIO: IO[NodeState]

  def run: IO[FiberIO[Unit]]

  def interrupt: IO[Unit]

  def appendEntries(request: AppendEntriesRequest): IO[Boolean]

  def vote(request: VoteRequest): IO[Boolean]

  def join(request: JoinRequest): IO[JoinResponse.Status]

  def header: IO[Header] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      state     <- stateIO
      h = Header(
        peer = state.self.some,
        messageId = messageId.value,
        term = state.currentTerm,
      )
    } yield h

}

object Node {

  private final val logger = scribe.cats[IO]

  def instance(
    context: NodeContext,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    state: NodeState
  ): IO[Node] =
    state match {
      case state: NodeState.Orphan =>
        for {
          s <- Deferred[IO, Unit]
          n <- orphan(context, events, supervisor, trace, s, state)
        } yield n
      case state: NodeState.NonVoter => ???
      case state: NodeState.Follower =>
        for {
          s <- Deferred[IO, Unit]
          n <- follower(context, events, supervisor, trace, s, state)
        } yield n
      case state: NodeState.Candidate =>
        for {
          s <- Deferred[IO, Unit]
          n <- candidate(context, events, supervisor, trace, s, state)
        } yield n
      case state: NodeState.Leader =>
        for {
          s <- Deferred[IO, Unit]
          n <- leader(context, events, supervisor, trace, s, state)
        } yield n
      case state: NodeState.Joint => ???
    }

  private def orphan(
    context: NodeContext,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    signal: Deferred[IO, Unit],
    state: NodeState.Orphan,
  ): IO[Node] =
    for {
      r <- Ref.of[IO, NodeState.Orphan](state)
      t <- trace.incrementOrphan
      n <- OrphanNode.instance(context, r, events, signal, supervisor, t)
    } yield n

  private def follower(
    context: NodeContext,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    signal: Deferred[IO, Unit],
    state: NodeState.Follower,
  ): IO[Node] =
    for {
      r <- Ref.of[IO, NodeState.Follower](state)
      t <- trace.incrementFollower
      n <- FollowerNode.instance(context, r, events, signal, supervisor, t)
    } yield n

  private def candidate(
    context: NodeContext,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    signal: Deferred[IO, Unit],
    state: NodeState.Candidate,
  ): IO[Node] =
    for {
      r <- Ref.of[IO, NodeState.Candidate](state)
      t <- trace.incrementCandidate
      n <- CandidateNode.instance(context, r, events, signal, supervisor, t)
    } yield n

  private def leader(
    context: NodeContext,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    signal: Deferred[IO, Unit],
    state: NodeState.Leader,
  ): IO[Node] =
    for {
      r <- Ref.of[IO, NodeState.Leader](state)
      t <- trace.incrementLeader
      n <- LeaderNode.instance(context, r, events, signal, supervisor, t)
    } yield n

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
        join(v)
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

  private def join(command: CliCommand.Join): IO[NodeState] = for {
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

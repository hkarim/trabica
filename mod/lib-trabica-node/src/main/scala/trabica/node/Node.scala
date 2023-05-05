package trabica.node

import cats.effect.*
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import trabica.model.*
import trabica.net.NodeApi
import trabica.store.FsmStore

trait Node {

  def context: NodeContext

  def quorumId: String

  def quorumPeer: Peer

  def stateIO: IO[NodeState]

  def run: IO[FiberIO[Unit]]

  def interrupt: IO[Unit]

  def prefix: String

  def appendEntries(request: AppendEntriesRequest): IO[Boolean]

  def vote(request: VoteRequest): IO[Boolean]

  def quorumNode: QuorumNode =
    QuorumNode(id = quorumId, peer = quorumPeer.some)

  def header: IO[Header] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      state     <- stateIO
      h = Header(
        node = Some(quorumNode),
        messageId = messageId.value,
        term = state.localState.currentTerm,
      )
    } yield h

  def header(state: NodeState): IO[Header] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      h = Header(
        node = Some(quorumNode),
        messageId = messageId.value,
        term = state.localState.currentTerm,
      )
    } yield h

  def quorum: IO[Quorum] =
    for {
      co <- context.store.configuration
      c <- co.required(
        NodeError.StoreError(s"$prefix configuration entry not found")
      )
      qo <- c.quorum
      q <- qo.required(
        NodeError.StoreError(s"$prefix quorum not found in latest configuration")
      )
    } yield q

  def quorumPeers: IO[Vector[Peer]] =
    for {
      q <- quorum
      ns = q.nodes.toVector.filterNot(_.id == quorumId)
      ps <- ns.traverse(n => n.peer.required(NodeError.StoreError("peers not found")))
    } yield ps

  def clients: Resource[IO, Vector[NodeApi]] =
    for {
      q <- Resource.eval(quorum)
      ns = q.nodes.toVector.filterNot(_.id == quorumId)
      clients <- ns.traverse { n =>
        for {
          p <- Resource.eval(n.peer.required(NodeError.StoreError(s"$prefix peer is required")))
          c <- context.networking.client(prefix, n.id, p)
        } yield c
      }
    } yield clients

}

object Node {

  private final val logger = scribe.cats[IO]

  def instance(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    state: NodeState.Follower
  ): IO[Node] =
    for {
      s <- Deferred[IO, Either[Throwable, Unit]]
      n <- follower(context, quorumId, quorumPeer, events, supervisor, trace, s, state)
    } yield n

  private def follower(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    events: Queue[IO, Event],
    supervisor: Supervisor[IO],
    trace: Ref[IO, NodeTrace],
    signal: Interrupt,
    state: NodeState.Follower,
  ): IO[Node] =
    for {
      r <- Ref.of[IO, NodeState.Follower](state)
      t <- trace.incrementFollower
      n <- FollowerNode.instance(context, quorumId, quorumPeer, r, events, signal, supervisor, t)
    } yield n

  def termCheck(header: Header, currentState: NodeState, events: Queue[IO, Event]): IO[Unit] =
    ???

  def state(command: CliCommand, store: FsmStore): IO[NodeState.Follower] = command match {
    case c: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(c, store)
    case _: CliCommand.Startup =>
      logger.debug("initiating node state in startup mode") >>
        startup(store)
  }

  private def bootstrap(command: CliCommand.Bootstrap, store: FsmStore): IO[NodeState.Follower] = {
    val quorumNode =
      QuorumNode(
        id = command.id,
        peer = Some(Peer(host = command.host, port = command.port))
      )
    val localState =
      LocalState(
        node = Some(quorumNode),
        currentTerm = 0,
        votedFor = None,
      )

    for {
      _ <- store.bootstrap
      _ <- store.writeState(localState)
      nodeState = NodeState.Follower(
        localState = localState,
        commitIndex = Index.zero,
        lastApplied = Index.zero,
      )
    } yield nodeState
  }

  private def startup(store: FsmStore): IO[NodeState.Follower] =
    for {
      localStateOption <- store.readState
      localState <- IO.fromOption(localStateOption)(
        NodeError.StoreError(
          "local state does not exist, restore the state manually or start in bootstrap mode"
        )
      )
      entryOption <- store.last
      index = entryOption.map(_.index).map(Index.of).getOrElse(Index.zero)
      nodeState = NodeState.Follower(
        localState = localState,
        commitIndex = index,
        lastApplied = index,
      )
    } yield nodeState

}

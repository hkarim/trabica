package trabica.node

import cats.effect.*
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import scribe.Scribe
import trabica.model.*
import trabica.net.NodeApi
import trabica.store.FsmStore

trait Node[S <: NodeState] {

  def logger: Scribe[IO]

  def context: NodeContext

  def quorumId: String

  def quorumPeer: Peer

  def state: Ref[IO, S]

  def events: Queue[IO, Event]

  given lens: NodeStateLens[S]

  def run: IO[FiberIO[Unit]]

  def interrupt: IO[Unit]

  def prefix: String

  def appendEntries(request: AppendEntriesRequest): IO[Boolean]

  def vote(request: VoteRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required(NodeError.InvalidMessage)
      _ <- logger.debug(
        s"$prefix vote requested",
        s"votedFor=${currentState.localState.votedFor},",
        s"term=${currentState.localState.currentTerm},",
        s"request.term=${header.term}"
      )
      result <-
        currentState.localState.votedFor match {
          case Some(node) =>
            IO.pure(request.header.flatMap(_.node).contains(node))
          case None =>
            val theirTermIsHigher = header.term > currentState.localState.currentTerm
            for {
              last <- context.store.last
              lastTerm  = last.map(_.term).getOrElse(0L)
              lastIndex = last.map(_.index).getOrElse(0L)
              ourLogIsBetter =
                lastTerm > request.lastLogTerm ||
                  (lastTerm == request.lastLogTerm && lastIndex > request.lastLogIndex)
              voteGranted <-
                if theirTermIsHigher && !ourLogIsBetter then {
                  for {
                    qn <- header.node.required(NodeError.InvalidMessage)
                    ls = currentState.localState.copy(votedFor = qn.some)
                    _ <- state.set(currentState.updated(localState = ls))
                    _ <- context.store.writeState(ls)
                  } yield true
                } else IO.pure(false)
              _ <- logger.debug(s"$prefix responding with voteGranted: $voteGranted")
            } yield voteGranted
        }
    } yield result

  def quorumNode: QuorumNode =
    QuorumNode(id = quorumId, peer = quorumPeer.some)

  def makeHeader: IO[Header] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      h = Header(
        node = Some(quorumNode),
        messageId = messageId.value,
        term = currentState.localState.currentTerm,
      )
    } yield h

  def makeHeader(state: NodeState): IO[Header] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      h = Header(
        node = Some(quorumNode),
        messageId = messageId.value,
        term = state.localState.currentTerm,
      )
    } yield h

  def makeFollowerState(currentState: NodeState, term: Long): NodeState.Follower =
    NodeState.Follower(
      localState = LocalState(
        node = QuorumNode(id = quorumId, peer = Some(quorumPeer)).some,
        currentTerm = term,
        votedFor = None,
      ),
      commitIndex = currentState.commitIndex,
      lastApplied = currentState.lastApplied
    )

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
  ): IO[Node[NodeState.Follower]] =
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
  ): IO[Node[NodeState.Follower]] =
    for {
      r <- Ref.of[IO, NodeState.Follower](state)
      t <- trace.incrementFollower
      n <- FollowerNode.instance(context, quorumId, quorumPeer, r, events, signal, supervisor, t)
    } yield n

  def state(command: CliCommand, store: FsmStore): IO[NodeState.Follower] = command match {
    case c: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(c, store)
    case c: CliCommand.Startup =>
      logger.debug("initiating node state in startup mode") >>
        startup(c, store)
  }

  private def bootstrap(command: CliCommand.Bootstrap, store: FsmStore): IO[NodeState.Follower] = {
    val quorumNode =
      QuorumNode(
        id = command.id,
        peer = Some(Peer(host = command.host, port = command.port))
      )
    val localState =
      LocalState(
        node = quorumNode.some,
        currentTerm = 0L,
        votedFor = None,
      )

    for {
      _ <- store.bootstrap // clear all current store managed files
      data = Quorum(nodes = command.quorumPeers :+ quorumNode).toByteString
      c <- store.append(LogEntry(index = 1L, term = 1L, tag = LogEntryTag.Conf, data = data))
      _ <- logger.debug(s"appended conf entry with result: $c")
      _ <- store.writeState(localState)
      nodeState = NodeState.Follower(
        localState = localState,
        commitIndex = Index.one,
        lastApplied = Index.one,
      )
    } yield nodeState
  }

  private def startup(command: CliCommand.Startup, store: FsmStore): IO[NodeState.Follower] =
    for {
      localStateOption <- store.readState
      quorumNode = QuorumNode(
        id = command.id,
        peer = Some(Peer(host = command.host, port = command.port))
      )
      localState = localStateOption match {
        case Some(value) =>
          value.copy(node = quorumNode.some)
        case None =>
          LocalState(
            node = quorumNode.some,
            currentTerm = 0,
            votedFor = None,
          )
      }
      _           <- store.writeState(localState)
      entryOption <- store.last
      index = entryOption.map(_.index).map(Index.of).getOrElse(Index.zero)
      nodeState = NodeState.Follower(
        localState = localState,
        commitIndex = index,
        lastApplied = index,
      )
    } yield nodeState

}

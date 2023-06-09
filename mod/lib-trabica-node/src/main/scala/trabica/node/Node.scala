package trabica.node

import cats.effect.*
import cats.syntax.all.*
import scribe.Scribe
import trabica.model.*
import trabica.net.NodeApi
import trabica.store.Log

trait Node[S <: NodeState] {

  def logger: Scribe[IO]

  def context: NodeContext

  def state: Ref[IO, S]

  given lens: NodeStateLens[S]

  def run: IO[FiberIO[Unit]]

  def interrupt: IO[Unit]

  def prefix: String

  def appendEntries(request: AppendEntriesRequest): IO[Boolean]

  def vote(request: VoteRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      _ <- logger.debug(
        s"$prefix vote requested",
        s"votedFor=${currentState.localState.votedFor},",
        s"currentTerm=${currentState.localState.currentTerm},",
        s"requestTerm=${header.term}"
      )
      candidateId <- header.node.required(NodeError.InvalidHeader)
      result <-
        currentState.localState.votedFor match {
          case Some(node) if node.id == candidateId.id =>
            logger.debug(s"$prefix already voted for ${candidateId.id}") >>
              IO.pure(true)
          case _ =>
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
                    qn <- header.node.required(NodeError.InvalidHeader)
                    ls = currentState.localState.copy(votedFor = qn.some)
                    _ <- state.set(currentState.updated(localState = ls))
                    _ <- context.store.writeState(ls)
                  } yield true
                } else IO.pure(false)
              _ <- logger.debug(s"$prefix responding with voteGranted: $voteGranted to peer ${candidateId.peer.show}")
            } yield voteGranted
        }
    } yield result

  def addServer(request: AddServerRequest): IO[AddServerResponse]

  def removeServer(request: RemoveServerRequest): IO[RemoveServerResponse]

  def member: Member =
    Member(id = context.memberId, peer = context.memberPeer.some)

  def makeHeader: IO[Header] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      h = Header(
        node = Some(member),
        messageId = messageId.value,
        term = currentState.localState.currentTerm,
      )
    } yield h

  def makeHeader(state: NodeState): IO[Header] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      h = Header(
        node = Some(member),
        messageId = messageId.value,
        term = state.localState.currentTerm,
      )
    } yield h

  def makeFollowerState(currentState: NodeState, term: Long, leader: Option[Member]): NodeState.Follower =
    NodeState.Follower(
      localState = LocalState(
        node = Member(id = context.memberId, peer = Some(context.memberPeer)).some,
        currentTerm = term,
        votedFor = None,
      ),
      commitIndex = currentState.commitIndex,
      lastApplied = currentState.lastApplied,
      leader = leader,
    )

  def cluster: IO[Cluster] =
    for {
      co <- context.store.configuration
      c <- co.required(
        NodeError.StoreError(s"$prefix configuration entry not found")
      )
      qo <- c.cluster
      q <- qo.required(
        NodeError.StoreError(s"$prefix quorum not found in latest configuration")
      )
    } yield q

  def clusterPeers: IO[Vector[Peer]] =
    for {
      q <- cluster
      ns = q.nodes.toVector.filterNot(_.id == context.memberId)
      ps <- ns.traverse(n => n.peer.required(NodeError.StoreError("peers not found")))
    } yield ps

  def clients: Resource[IO, Vector[NodeApi]] =
    for {
      q <- Resource.eval(cluster)
      ns = q.nodes.toVector.filterNot(_.id == context.memberId)
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
    trace: Ref[IO, NodeTrace],
    state: NodeState.Follower
  ): IO[Node[NodeState.Follower]] =
    for {
      s <- Deferred[IO, Either[Throwable, Unit]]
      n <- follower(context, trace, s, state)
    } yield n

  private def follower(
    context: NodeContext,
    trace: Ref[IO, NodeTrace],
    signal: Interrupt,
    state: NodeState.Follower,
  ): IO[Node[NodeState.Follower]] =
    for {
      r <- Ref.of[IO, NodeState.Follower](state)
      t <- trace.incrementFollower
      n <- FollowerNode.instance(context, r, signal, t)
    } yield n

  def state(command: CliCommand, store: Log): IO[NodeState.Follower] = command match {
    case c: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(c, store)
    case c: CliCommand.Startup =>
      logger.debug("initiating node state in startup mode") >>
        startup(c, store)
  }

  private def bootstrap(command: CliCommand.Bootstrap, store: Log): IO[NodeState.Follower] = {
    val member =
      Member(
        id = command.id,
        peer = Some(Peer(host = command.host, port = command.port))
      )
    val localState =
      LocalState(
        node = member.some,
        currentTerm = 0L,
        votedFor = None,
      )

    for {
      _ <- store.bootstrap // clear all current store managed files
      data = Cluster(nodes = command.members :+ member).toByteString
      c <- store.append(LogEntry(index = 1L, term = 0L, tag = LogEntryTag.Conf, data = data))
      _ <- logger.debug(s"appended conf entry with result: $c")
      _ <- store.writeState(localState)
      nodeState = NodeState.Follower(
        localState = localState,
        commitIndex = Index.one,
        lastApplied = Index.one,
        leader = None,
      )
    } yield nodeState
  }

  private def startup(command: CliCommand.Startup, store: Log): IO[NodeState.Follower] =
    for {
      localStateOption <- store.readState
      quorumNode = Member(
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
        leader = None,
      )
    } yield nodeState

}

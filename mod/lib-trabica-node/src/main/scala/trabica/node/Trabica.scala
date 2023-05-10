package trabica.node

import cats.effect.*
import cats.effect.std.{Mutex, Queue, Supervisor}
import cats.syntax.all.*
import com.typesafe.config.ConfigFactory
import fs2.*
import trabica.model.*
import trabica.net.{Networking, NodeApi}
import trabica.store.FsmStore

class Trabica(
  val context: NodeContext,
  val ref: Ref[IO, Node[_ <: NodeState]],
  val mutex: Mutex[IO],
  val trace: Ref[IO, NodeTrace],
) extends NodeApi {

  private final val logger = scribe.cats[IO]

  override final val quorumId: String = context.quorumId

  override final val quorumPeer: Peer = context.quorumPeer

  def run: IO[Unit] =
    eventStream

  private def transition(oldState: NodeState, newState: NodeState, reason: StateTransitionReason): IO[FiberIO[Unit]] =
    for {
      _      <- logger.debug(s"transitioning [from: ${oldState.tag}, to: ${newState.tag}, reason: $reason]")
      signal <- Interrupt.instance
      f <- newState match {
        case state: NodeState.Follower =>
          follower(state, signal).supervise(context.supervisor)
        case state: NodeState.Candidate =>
          candidate(state, signal).supervise(context.supervisor)
        case state: NodeState.Leader =>
          leader(state, signal).supervise(context.supervisor)
      }
    } yield f

  private def eventStream: IO[Unit] =
    Stream
      .fromQueueUnterminated(context.events)
      .evalMap {
        case Event.NodeStateChanged(oldState, newState, reason) =>
          logger.debug(s"[event] node state changed, transitioning") >>
            transition(oldState, newState, reason)
      }
      .compile
      .drain

  private def startup[S <: NodeState](
    newNode: Node[S],
    signal: Interrupt,
    loggingPrefix: String,
  ): IO[Unit] = for {
    f <- ref.flatModify { oldNode =>
      val io = for {
        _ <- logger.debug(s"interrupting current node")
        _ <- oldNode.interrupt
        _ <- logger.debug(s"$loggingPrefix starting node transition")
        spawned <- mutex.lock.surround {
          for {
            s <- newNode.state.get
            _ <- logger.debug(s"$loggingPrefix writing persistent state to disk")
            _ <- context.store.writeState(s.localState)
            _ <- logger.debug(s"$loggingPrefix starting")
            r <- newNode.run
          } yield r
        }
      } yield spawned
      (newNode, io)
    }
    _ <- logger.debug(s"$loggingPrefix scheduled, awaiting terminate signal")
    _ <- signal.get
    o <- f.join
    _ <- o match {
      case Outcome.Succeeded(_) =>
        logger.debug(s"$loggingPrefix terminated successfully")
      case Outcome.Errored(e) =>
        logger.debug(s"$loggingPrefix terminated with error ${e.getMessage}")
      case Outcome.Canceled() =>
        logger.debug(s"$loggingPrefix canceled")
    }
  } yield ()

  private def follower(newState: NodeState.Follower, s: Interrupt): IO[Unit] =
    for {
      r <- Ref.of[IO, NodeState.Follower](newState)
      t <- trace.incrementFollower
      l = s"[follower-${t.followerId}]"
      n <- FollowerNode.instance(context, r, s, t)
      _ <- startup(n, s, l)
    } yield ()

  private def candidate(newState: NodeState.Candidate, s: Interrupt): IO[Unit] =
    for {
      r <- Ref.of[IO, NodeState.Candidate](newState)
      t <- trace.incrementCandidate
      l = s"[candidate-${t.candidateId}]"
      n <- CandidateNode.instance(context, r, s, t)
      _ <- startup(n, s, l)
    } yield ()

  private def leader(newState: NodeState.Leader, s: Interrupt): IO[Unit] =
    for {
      r <- Ref.of[IO, NodeState.Leader](newState)
      t <- trace.incrementLeader
      l = s"[leader-${t.leaderId}]"
      n <- LeaderNode.instance(context, r, s, t)
      _ <- startup(n, s, l)
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse] = {
    def makeResponse(header: Header, success: Boolean): AppendEntriesResponse =
      AppendEntriesResponse(
        header = header.some,
        success = success,
      )

    for {
      server        <- ref.get
      currentState  <- server.state.get
      requestHeader <- request.header.required(NodeError.InvalidMessage)
      outdated = requestHeader.term > currentState.localState.currentTerm
      response <-
        if outdated && currentState.tag == NodeStateTag.Follower then {
          // higher term detected, current state is follower
          // update the term and proceed normally
          val ls       = currentState.localState.copy(currentTerm = requestHeader.term)
          val newState = currentState.updated(ls)(using server.lens)
          for {
            _ <- server.state.set(newState)
            _ <- context.store.writeState(ls)
            s <- server.appendEntries(request)
            h <- server.makeHeader
            r = makeResponse(h, s)
          } yield r
        } else if outdated && currentState.tag != NodeStateTag.Follower then {
          // higher term detected, current state is not follower
          // convert to follower and restart operations
          // response with failure and don't append the entry for now
          val followerState = server.makeFollowerState(currentState, requestHeader.term)
          val event = Event.NodeStateChanged(
            oldState = currentState,
            newState = followerState,
            reason = StateTransitionReason.HigherTermDiscovered,
          )
          for {
            h <- server.makeHeader
            r = makeResponse(h, false)
            _ <- context.events.offer(event)
          } yield r
        } else {
          // term is OK, proceed normally
          for {
            success <- server.appendEntries(request)
            h       <- server.makeHeader
            r = makeResponse(h, success)
          } yield r
        }
    } yield response
  }

  override def vote(request: VoteRequest): IO[VoteResponse] =
    for {
      server      <- ref.get
      header      <- server.makeHeader
      voteGranted <- server.vote(request)
      response = VoteResponse(
        header = header.some,
        voteGranted = voteGranted,
      )
    } yield response

}

object Trabica {

  private final val logger = scribe.cats[IO]

  private def logging(level: scribe.Level): IO[Unit] = IO.delay {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(level))
      .replace()
  }.void

  def run(supervisor: Supervisor[IO], command: CliCommand, store: FsmStore, networking: Networking): IO[Unit] =
    for {
      config    <- IO.blocking(ConfigFactory.load())
      _         <- logging(scribe.Level(config.getString("trabica.log.level")))
      messageId <- Ref.of[IO, MessageId](MessageId.zero)
      state     <- Node.state(command, store)
      quorumNode <- state.localState.node.required(
        NodeError.StoreError("`localState.node` configuration not found in local state")
      )
      quorumPeer <- quorumNode.peer.required(
        NodeError.StoreError("`localState.node.peer` configuration not found in local state")
      )
      events <- Queue.unbounded[IO, Event]
      context = NodeContext(
        config = config,
        messageId = messageId,
        networking = networking,
        events = events,
        supervisor = supervisor,
        store = store,
        quorumId = quorumNode.id,
        quorumPeer = quorumPeer,
      )
      trace <- Ref.of[IO, NodeTrace](NodeTrace.instance)
      node  <- Node.instance(context, trace, state)
      ref   <- Ref.of[IO, Node[_ <: NodeState]](node)
      mutex <- Mutex[IO]
      trabica = new Trabica(context, ref, mutex, trace)
      _ <- node.run.supervise(supervisor)
      _ <- logger.info(s"starting up in mode follower")
      _ <- trabica.run.supervise(supervisor)
      _ <- networking.server(trabica, quorumPeer.host, quorumPeer.port).useForever
    } yield ()

}

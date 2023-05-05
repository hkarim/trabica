package trabica.node

import cats.effect.*
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import com.typesafe.config.ConfigFactory
import fs2.*
import trabica.model.*
import trabica.net.{Networking, NodeApi}
import trabica.store.FsmStore

class Trabica(
  val context: NodeContext,
  val quorumId: String,
  val quorumPeer: Peer,
  val ref: Ref[IO, Node],
  val events: Queue[IO, Event],
  val supervisor: Supervisor[IO],
  val trace: Ref[IO, NodeTrace],
) extends NodeApi {

  private final val logger = scribe.cats[IO]

  def run: IO[Unit] =
    eventStream

  private def transition(oldState: NodeState, newState: NodeState, reason: StateTransitionReason): IO[FiberIO[Unit]] =
    for {
      _      <- logger.debug(s"transitioning [from: ${oldState.tag}, to: ${newState.tag}, reason: $reason]")
      signal <- Interrupt.instance
      f <- newState match {
        case state: NodeState.NonVoter => ???
        case state: NodeState.Follower =>
          follower(state, signal).supervise(supervisor)
        case state: NodeState.Candidate =>
          candidate(state, signal).supervise(supervisor)
        case state: NodeState.Leader =>
          leader(state, signal).supervise(supervisor)
      }
    } yield f

  private def eventStream: IO[Unit] =
    Stream
      .fromQueueUnterminated(events)
      .evalMap {
        case Event.NodeStateChanged(oldState, newState, reason) =>
          logger.debug(s"[event] node state changed, transitioning") >>
            transition(oldState, newState, reason)
      }
      .compile
      .drain

  private def startup(
    newNode: Node,
    signal: Interrupt,
    loggingPrefix: String,
  ): IO[Unit] = for {
    f <- ref.flatModify { oldNode =>
      val io = for {
        _       <- logger.debug(s"interrupting current node")
        _       <- oldNode.interrupt
        _       <- logger.debug(s"$loggingPrefix starting node transition")
        spawned <- newNode.run
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
      n <- FollowerNode.instance(context, quorumId, quorumPeer, r, events, s, supervisor, t)
      _ <- startup(n, s, l)
    } yield ()

  private def candidate(newState: NodeState.Candidate, s: Interrupt): IO[Unit] =
    for {
      r <- Ref.of[IO, NodeState.Candidate](newState)
      t <- trace.incrementCandidate
      l = s"[candidate-${t.candidateId}]"
      n <- CandidateNode.instance(context, quorumId, quorumPeer, r, events, s, supervisor, t)
      _ <- startup(n, s, l)
    } yield ()

  private def leader(newState: NodeState.Leader, s: Interrupt): IO[Unit] =
    for {
      r <- Ref.of[IO, NodeState.Leader](newState)
      t <- trace.incrementLeader
      l = s"[leader-${t.leaderId}]"
      n <- LeaderNode.instance(context, quorumId, quorumPeer, r, events, s, supervisor, t)
      _ <- startup(n, s, l)
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse] =
    for {
      server  <- ref.get
      header  <- server.header
      success <- server.appendEntries(request)
      response = AppendEntriesResponse(
        header = header.some,
        success = success,
      )
    } yield response

  override def vote(request: VoteRequest): IO[VoteResponse] =
    for {
      server      <- ref.get
      header      <- server.header
      voteGranted <- server.vote(request)
      response = VoteResponse(
        header = header.some,
        voteGranted = voteGranted,
      )
    } yield response

}

object Trabica {

  private def logging(level: scribe.Level): IO[Unit] = IO.delay {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(level))
      .replace()
  }.void

  def run(command: CliCommand, store: FsmStore, networking: Networking): IO[Unit] =
    Supervisor[IO](await = false).use { supervisor =>
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
        context = NodeContext(
          config = config,
          messageId = messageId,
          store = store,
          networking = networking,
        )
        events <- Queue.unbounded[IO, Event]
        trace  <- Ref.of[IO, NodeTrace](NodeTrace.instance)
        node   <- Node.instance(context, quorumNode.id, quorumPeer, events, supervisor, trace, state)
        ref    <- Ref.of[IO, Node](node)
        trabica = new Trabica(context, quorumNode.id, quorumPeer, ref, events, supervisor, trace)
        _ <- node.run.supervise(supervisor)
        _ <- trabica.run.supervise(supervisor)
        _ <- networking.server(trabica, quorumPeer.host, quorumPeer.port).useForever
      } yield ()
    }

}
package trabica.node

import cats.effect.*
import cats.effect.std.{Mutex, Queue, Supervisor}
import com.typesafe.config.ConfigFactory
import io.grpc.Metadata
import fs2.*
import trabica.model.{CliCommand, Event, MessageId, NodeState}
import trabica.net.GrpcServer
import trabica.rpc.*

class Trabica(
  val context: NodeContext,
  val ref: Ref[IO, Node],
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
        currentNode <- ref.get
        f <- newState match {
          case state: NodeState.Orphan =>
            logger.debug("transitioning to orphan") >>
              orphan(currentNode, state).supervise(supervisor)
          case state: NodeState.NonVoter => ???
          case state: NodeState.Follower =>
            logger.debug("transitioning to follower") >>
              follower(currentNode, state).supervise(supervisor)
          case state: NodeState.Candidate =>
            logger.debug("transitioning to candidate") >>
              candidate(currentNode, state).supervise(supervisor)
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

  private def startup(
    currentNode: Node,
    newNode: Node,
    signal: Deferred[IO, Unit],
    loggingPrefix: String,
  ): IO[Unit] = for {
    _ <- logger.debug(s"$loggingPrefix starting node transition")
    _ <- logger.debug(s"$loggingPrefix interrupting current node")
    _ <- currentNode.interrupt
    f <- ref.flatModify(_ => (newNode, newNode.run))
    _ <- logger.debug(s"$loggingPrefix scheduled, awaiting terminate signal")
    _ <- signal.get
    _ <- f.cancel
    _ <- logger.debug(s"$loggingPrefix terminated")
  } yield ()

  private def orphan(currentNode: Node, newState: NodeState.Orphan): IO[Unit] =
    for {
      s <- Deferred[IO, Unit]
      r <- Ref.of[IO, NodeState.Orphan](newState)
      t <- trace.incrementOrphan
      l = s"[orphan-${t.orphanId}]"
      n <- OrphanNode.instance(context, r, events, s, supervisor, t)
      _ <- startup(currentNode, n, s, l)
    } yield ()

  private def follower(currentNode: Node, newState: NodeState.Follower): IO[Unit] =
    for {
      s <- Deferred[IO, Unit]
      r <- Ref.of[IO, NodeState.Follower](newState)
      t <- trace.incrementFollower
      l = s"[follower-${t.followerId}]"
      n <- FollowerNode.instance(context, r, events, s, supervisor, t)
      _ <- startup(currentNode, n, s, l)
    } yield ()

  private def candidate(currentNode: Node, newState: NodeState.Candidate): IO[Unit] =
    for {
      s <- Deferred[IO, Unit]
      r <- Ref.of[IO, NodeState.Candidate](newState)
      t <- trace.incrementCandidate
      l = s"[candidate-${t.candidateId}]"
      n <- CandidateNode.instance(context, r, events, s, supervisor, t)
      _ <- startup(currentNode, n, s, l)
    } yield ()

  private def leader(currentNode: Node, newState: NodeState.Leader): IO[Unit] =
    for {
      s <- Deferred[IO, Unit]
      r <- Ref.of[IO, NodeState.Leader](newState)
      t <- trace.incrementLeader
      l = s"[leader-${t.leaderId}]"
      n <- LeaderNode.instance(context, r, events, s, supervisor, t)
      _ <- startup(currentNode, n, s, l)
    } yield ()

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      server   <- ref.get
      response <- server.appendEntries(request, metadata)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      server   <- ref.get
      response <- server.vote(request, metadata)
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      server   <- ref.get
      response <- server.join(request, metadata)
    } yield response
}

object Trabica {

  private def instance(context: NodeContext, ref: Ref[IO, Node], supervisor: Supervisor[IO]): IO[Trabica] =
    for {
      trace  <- Ref.of[IO, NodeTrace](NodeTrace.instance)
      events <- Queue.unbounded[IO, Event]
      mutex  <- Mutex[IO]
      trabica = new Trabica(context, ref, events, supervisor, mutex, trace)
    } yield trabica

  private def logging(level: scribe.Level): IO[Unit] = IO.delay {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(level))
      .replace()
  }.void

  def run(command: CliCommand): IO[Unit] =
    Supervisor[IO].use { supervisor =>
      for {
        config    <- IO.blocking(ConfigFactory.load())
        _         <- logging(scribe.Level(config.getString("trabica.log.level")))
        messageId <- Ref.of[IO, MessageId](MessageId.zero)
        context = NodeContext(
          config = config,
          messageId = messageId,
        )
        ref     <- Ref.of[IO, Node](Node.DeadNode)
        trabica <- Trabica.instance(context, ref, supervisor)
        fiber   <- trabica.run.supervise(supervisor)
        state   <- Node.state(command)
        _       <- trabica.events.offer(Event.NodeStateChanged(state))
        _       <- GrpcServer.resource(trabica, command).useForever
        _       <- fiber.cancel
      } yield ()
    }

}

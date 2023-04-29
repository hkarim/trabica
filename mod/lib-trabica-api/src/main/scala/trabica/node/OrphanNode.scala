package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.{Event, NodeState}
import trabica.net.GrpcClient
import trabica.rpc.*
import trabica.rpc.JoinResponse.Status

import scala.concurrent.duration.*

class OrphanNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Orphan],
  val events: Queue[IO, Event],
  val signal: Deferred[IO, Unit],
  val streamSignal: SignallingRef[IO, Boolean],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.orphanId

  private final val prefix: String = s"[orphan-$id]"

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(prefix, peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Orphan): IO[FiberIO[Unit]] =
    for {
      _ <- logger.debug(s"$prefix peers changed, restarting join stream")
      _ <- streamSignal.set(true) // stop the stream
      _ <- state.set(newState)
      _ <- streamSignal.set(false)                       // reset the signal
      f <- clients.use(joinStream).supervise(supervisor) // start the stream
    } yield f

  private def joinStream(clients: Vector[TrabicaFs2Grpc[IO, Metadata]]): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(streamSignal)
      .flatMap { _ =>
        Stream.eval {
          for {
            messageId <- context.messageId.getAndUpdate(_.increment)
            s         <- state.get
            request = JoinRequest(
              header = Header(
                peer = s.self.some,
                messageId = messageId.value,
                term = s.currentTerm,
              ).some
            )
            responses <- clients.parTraverse { c =>
              c.join(request, new Metadata)
                .timeout(100.milliseconds)
                .attempt
                .flatMap(onJoin)
            }
          } yield responses
        }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in join stream: ${e.getMessage}", e)
        }
      }
      .onFinalize {
        logger.debug(s"$prefix join stream stopped")
      }
      .compile
      .drain

  private def onJoin(response: Either[Throwable, JoinResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"$prefix no response ${e.getMessage}")
      case Right(r) =>
        r.status match {
          case Status.Empty =>
            IO.unit
          case Status.Accepted(_) =>
            for {
              currentState <- state.get
              header       <- r.header.required
              peer         <- header.peer.required
              _            <- logger.debug(s"$prefix accepted by peer ${peer.host}:${peer.port}")
              newState = NodeState.Follower(
                id = currentState.id,
                self = currentState.self,
                peers = Set(peer),
                leader = peer,
                currentTerm = header.term,
                votedFor = None,
                commitIndex = currentState.commitIndex,
                lastApplied = currentState.lastApplied,
              )
              _ <- events.offer(Event.NodeStateChanged(newState))
            } yield ()
          case Status.Forward(JoinResponse.Forward(leaderOption, _)) =>
            for {
              header       <- r.header.required
              peer         <- header.peer.required
              leader       <- leaderOption.required
              currentState <- state.get
              _            <- logger.debug(s"$prefix forwarded to leader ${leader.host}:${leader.port}")
              newState = currentState.copy(
                peers = currentState.peers + leader - peer - currentState.self
              ) // change the peers
              _ <- peersChanged(newState)
            } yield ()
          case Status.UnknownLeader(JoinResponse.UnknownLeader(knownPeers, _)) =>
            for {
              header       <- r.header.required
              peer         <- header.peer.required
              _            <- logger.debug(s"$prefix updating peers through peer ${peer.host}:${peer.port}")
              currentState <- state.get
              newState = currentState.copy(
                peers = Set.from(knownPeers) - peer - currentState.self
              ) // change the peers
              _ <- peersChanged(newState)
            } yield ()
        }
    }

  def run: IO[FiberIO[Unit]] =
    clients.use(joinStream).supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    IO.pure(false)

  override def vote(request: VoteRequest): IO[Boolean] =
    IO.pure(false)

  override def join(request: JoinRequest): IO[JoinResponse.Status] =
    for {
      s <- state.get
      status = JoinResponse.Status.UnknownLeader(
        JoinResponse.UnknownLeader(knownPeers = s.peers.toSeq)
      )
    } yield status

}

object OrphanNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Orphan],
    events: Queue[IO, Event],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[OrphanNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new OrphanNode(
      context,
      state,
      events,
      signal,
      streamSignal,
      supervisor,
      trace,
    )
  } yield node

}

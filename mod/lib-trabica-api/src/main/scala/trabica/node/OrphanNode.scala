package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*

import scala.concurrent.duration.*

class OrphanNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Orphan],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.orphanId

  private final val prefix: String = s"[orphan-$id]"

  override def interrupt: IO[Unit] =
    signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def clients: Resource[IO, Vector[NodeApi]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        NodeApi.client(prefix, peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Orphan): IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix peers changed, restarting join stream")
      currentState <- state.get
      _            <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.ConfigurationChanged))
    } yield ()

  private def joinStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(signal)
      .flatMap { _ =>
        Stream.eval {
          for {
            h <- header
            request = JoinRequest(header = h.some)
            responses <- clients.parTraverse { c =>
              c.join(request)
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
        logger.debug(s"$prefix join stream finalized")
      }
      .compile
      .drain

  private def onJoin(response: Either[Throwable, JoinResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"$prefix no response ${e.getMessage}")
      case Right(r) =>
        r.status match {
          case JoinResponse.Status.Empty =>
            IO.unit
          case JoinResponse.Status.Accepted(_) =>
            for {
              currentState <- state.get
              header       <- r.header.required
              peer         <- header.peer.required
              _            <- logger.debug(s"$prefix accepted by peer ${peer.host}:${peer.port}")
              newState = NodeState.Follower(
                self = currentState.self,
                peers = Set(peer),
                leader = peer,
                currentTerm = Term.of(header.term),
                votedFor = None,
                commitIndex = currentState.commitIndex,
                lastApplied = currentState.lastApplied,
              )
              _ <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.JoinAccepted))
            } yield ()
          case JoinResponse.Status.Forward(JoinResponse.Forward(leaderOption, _)) =>
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
          case JoinResponse.Status.UnknownLeader(JoinResponse.UnknownLeader(knownPeers, _)) =>
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
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[OrphanNode] = IO.pure {
    new OrphanNode(
      context,
      state,
      events,
      signal,
      supervisor,
      trace,
    )
  }

}

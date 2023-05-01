package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

class FollowerNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Follower],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val heartbeat: Ref[IO, Option[Unit]],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.followerId

  private final val prefix: String = s"[follower-$id]"

  private final val heartbeatStreamTimeoutMin: Long =
    context.config.getLong("trabica.follower.heartbeat-stream.timeout.min")

  private final val heartbeatStreamTimeoutMax: Long =
    context.config.getLong("trabica.follower.heartbeat-stream.timeout.max")

  override def stateIO: IO[NodeState] = state.get

  def run: IO[FiberIO[Unit]] =
    Stream
      .eval(Random.scalaUtilRandom[IO])
      .evalMap(_.betweenLong(heartbeatStreamTimeoutMin, heartbeatStreamTimeoutMax))
      .flatMap(heartbeatStream)
      .compile
      .drain
      .supervise(supervisor)

  override def interrupt: IO[Unit] =
    signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  private def heartbeatStream(rate: Long) =
    Stream
      .fixedRate[IO](rate.milliseconds)
      .interruptWhen(signal)
      .evalTap(_ => logger.trace(s"$prefix heartbeat wake up"))
      .evalMap { _ =>
        heartbeat.flatModify[Unit] {
          case Some(_) =>
            (None, logger.trace(s"$prefix heartbeat good to go"))
          case None =>
            (
              None,
              logger.debug(s"$prefix heartbeat no elements, will timeout") >>
                timeout
            )
        }
      }
      .handleErrorWith { e =>
        Stream.eval {
          e match {
            case _: TimeoutException =>
              timeout
            case e =>
              logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e) >>
                IO.raiseError(e)
          }
        }
      }
      .onFinalize {
        logger.debug(s"$prefix heartbeat stream finalized")
      }

  private def timeout: IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix heartbeat stream timed out")
      currentState <- state.get
      _            <- logger.debug(s"$prefix switching to candidate, ${currentState.peers.size} peer(s) known")
      newState = NodeState.Candidate(
        self = currentState.self,
        peers = currentState.peers,
        currentTerm = currentState.currentTerm.increment,
        votedFor = currentState.self.some, // vote for self
        commitIndex = currentState.commitIndex,
        lastApplied = currentState.lastApplied,
        votes = Set(currentState.self),
        elected = false,
      )
      - <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.NoHeartbeat))
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      _            <- heartbeat.set(Some(()))
      currentState <- state.get
      header       <- request.header.required
      peer         <- header.peer.required
      _ <-
        if !currentState.peers.contains(peer) then
          logger.debug(s"$prefix updating peers, ${request.peers.length} peer(s) known to leader") >>
            state.set(currentState.copy(peers = request.peers.toSet + peer - currentState.self))
        else IO.unit
      _ <- Node.termCheck(header, currentState, events)
    } yield false

  override def join(request: JoinRequest): IO[JoinResponse.Status] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} joining")
      currentState <- state.get
      status = JoinResponse.Status.Forward(
        JoinResponse.Forward(leader = currentState.leader.some)
      )
    } yield status

  override def vote(request: VoteRequest): IO[Boolean] =
    state.flatModify { currentState =>
      request.header match {
        case Some(header) =>
          header.peer match {
            case Some(peer) if currentState.votedFor.isEmpty && Term.of(header.term) > currentState.currentTerm =>
              (currentState.copy(votedFor = peer.some), IO.pure(true))
            case _ =>
              (currentState, IO.pure(false))
          }
        case None =>
          (currentState, IO.pure(false))
      }
    }

}

object FollowerNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Follower],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[FollowerNode] = for {
    heartbeat <- Ref.of[IO, Option[Unit]](None)
    node = new FollowerNode(
      context,
      state,
      events,
      signal,
      heartbeat,
      supervisor,
      trace,
    )
  } yield node
}

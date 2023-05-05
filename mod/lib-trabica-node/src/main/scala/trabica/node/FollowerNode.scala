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
  val quorumId: String,
  val quorumPeer: Peer,
  val state: Ref[IO, NodeState.Follower],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val heartbeat: Ref[IO, Option[Unit]],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.followerId

  override final val prefix: String = s"[follower-$id]"

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
      peers        <- quorumPeers
      _            <- logger.debug(s"$prefix switching to candidate, ${peers.size} peer(s) known")
      newTerm = currentState.localState.currentTerm + 1
      qn      = quorumNode
      newState = NodeState.Candidate(
        localState = LocalState(
          node = qn.some,
          currentTerm = newTerm,
          votedFor = qn.some,
        ),
        commitIndex = currentState.commitIndex,
        lastApplied = currentState.lastApplied,
        votingTerm = Term.of(newTerm),
        votes = Set(qn),
        elected = false,
      )
      - <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.NoHeartbeat))
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    ???

  override def vote(request: VoteRequest): IO[Boolean] =
    ???

}

object FollowerNode {
  def instance(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    state: Ref[IO, NodeState.Follower],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[FollowerNode] = for {
    heartbeat <- Ref.of[IO, Option[Unit]](None)
    node = new FollowerNode(
      context,
      quorumId,
      quorumPeer,
      state,
      events,
      signal,
      heartbeat,
      supervisor,
      trace,
    )
  } yield node
}
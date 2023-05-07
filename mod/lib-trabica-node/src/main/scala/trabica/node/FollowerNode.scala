package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*
import trabica.store.AppendResult

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
) extends Node[NodeState.Follower] {

  override final val logger = scribe.cats[IO]

  private final val id: Int = trace.followerId

  override final val prefix: String = s"[follower-$id]"

  private final val heartbeatStreamTimeoutMin: Long =
    context.config.getLong("trabica.follower.heartbeat-stream.timeout.min")

  private final val heartbeatStreamTimeoutMax: Long =
    context.config.getLong("trabica.follower.heartbeat-stream.timeout.max")

  override def lens: NodeStateLens[NodeState.Follower] =
    NodeStateLens[NodeState.Follower]

  override def run: IO[FiberIO[Unit]] =
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
                tryTimeout
            )
        }
      }
      .handleErrorWith { e =>
        Stream.eval {
          e match {
            case _: TimeoutException =>
              tryTimeout
            case e =>
              logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e) >>
                IO.raiseError(e)
          }
        }
      }
      .onFinalize {
        logger.debug(s"$prefix heartbeat stream finalized")
      }

  private def tryTimeout: IO[Unit] =
    for {
      peers <- quorumPeers.handleErrorWith { e =>
        logger.debug(s"$prefix canceling timeout, no peers available: ${e.getMessage}") >>
          IO.pure(Vector.empty)
      }
      currentState <- state.get
      _            <- logger.debug(s"$prefix current peers: $peers")
      // _            <- if peers.nonEmpty && currentState.localState.votedFor.isEmpty then timeout else IO.unit
      _ <- if peers.nonEmpty then timeout else IO.unit
    } yield ()

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
        votes = Set(qn),
        elected = false,
      )
      _ <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.NoHeartbeat))
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      _            <- heartbeat.set(Some(()))
      header       <- request.header.required(NodeError.InvalidMessage)
      currentState <- state.get
      termOK     = currentState.localState.currentTerm <= header.term
      firstEntry = (request.prevLogIndex == 0) && (request.prevLogTerm == 0)
      logOk <- context.store.contains(Index.of(request.prevLogIndex), Term.of(request.prevLogTerm))
      check = termOK && (firstEntry || logOk)
      _ <-
        if request.entries.nonEmpty then
          logger.debug(
            s"$prefix - size: ${request.entries.size}",
            s"check: $check",
            s"termOk: $termOK",
            s"firstEntry: $firstEntry",
            s"logOk: $logOk"
          )
        else IO.unit
      results <-
        if check then {
          request.entries.toVector.traverse { entry =>
            context.store.append(entry).flatMap {
              case AppendResult.IndexExistsWithTermConflict(storeTerm, incomingTerm) =>
                val message =
                  logger.debug(
                    s"$prefix failed to append entry - IndexExistsWithTermConflict",
                    s"incoming entry $entry",
                    s"storeTerm: $storeTerm, incomingTerm: $incomingTerm"
                  )
                for {
                  _ <- message
                  _ <- logger.debug(s"$prefix truncating up to and including ${entry.index}")
                  _ <- context.store.truncate(upToIncluding = Index.of(entry.index))
                  _ <- logger.debug(s"$prefix appending entry at ${entry.index}")
                  r <- context.store.append(entry)
                  _ <- logger.debug(s"$prefix append result $r")
                  _ <-
                    if r == AppendResult.Appended then
                      updateCommitIndex(request.commitIndex, entry.index)
                    else IO.unit
                } yield r == AppendResult.Appended
              case AppendResult.HigherTermExists(storeTerm, incomingTerm) =>
                logger.debug(
                  s"$prefix failed to append entry - HigherTermExists",
                  s"incoming entry $entry",
                  s"storeTerm: $storeTerm, incomingTerm: $incomingTerm"
                ) >> IO.pure(false)
              case AppendResult.NonMonotonicIndex(storeIndex, incomingIndex) =>
                logger.debug(
                  s"$prefix failed to append entry - NonMonotonicIndex",
                  s"incoming entry $entry",
                  s"storeIndex: $storeIndex, incomingIndex: $incomingIndex"
                ) >> IO.pure(false)
              case AppendResult.IndexExists =>
                logger.debug(
                  s"$prefix appending entry: IndexExists",
                ) >> IO.pure(true)
              case AppendResult.Appended =>
                for {
                  _ <- logger.debug(s"$prefix appending entry: Appended")
                  _ <- updateCommitIndex(request.commitIndex, entry.index)
                } yield true
            }
          }
        } else IO.pure(Vector(false))
      response = results.forall(identity)
    } yield response

  private def updateCommitIndex(leaderCommitIndex: Long, lastIndex: Long): IO[Unit] =
    state.update { currentState =>
      if leaderCommitIndex > currentState.commitIndex.value then
        currentState.copy(commitIndex = Index.of(math.min(leaderCommitIndex, lastIndex)))
      else
        currentState
    }

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

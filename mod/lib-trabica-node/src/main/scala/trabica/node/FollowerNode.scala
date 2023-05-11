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
  val state: Ref[IO, NodeState.Follower],
  val signal: Interrupt,
  val heartbeat: Ref[IO, Option[Unit]],
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
      .supervise(context.supervisor)

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
      _            <- logger.debug(s"$prefix current peers: ${peers.show}")
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
        leader = None,
      )
      _ <- context.events.offer(
        Event.NodeStateChanged(
          oldState = currentState,
          newState = newState,
          reason = StateTransitionReason.NoHeartbeat
        )
      )
    } yield ()

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      _            <- heartbeat.set(Some(()))
      header       <- request.header.required
      currentState <- state.get
      termOK     = currentState.localState.currentTerm <= header.term
      firstEntry = (request.prevLogIndex == 0) && (request.prevLogTerm == 0)
      logOk <- context.store.contains(Index.of(request.prevLogIndex), Term.of(request.prevLogTerm))
      check = termOK && (firstEntry || logOk)
      _ <-
        if request.entries.nonEmpty then
          logger.debug(
            s"$prefix append-entries",
            s"currentTerm: ${currentState.localState.currentTerm}",
            s"requestTerm: ${header.term}",
            s"termOk: $termOK",
            s"firstEntry: $firstEntry",
            s"logOk: $logOk",
            s"size: ${request.entries.size}",
            s"check: $check",
          )
        else IO.unit
      results <-
        if check then {
          request.entries.toVector.traverse { entry =>
            state.set(currentState.copy(leader = header.node)) >> // update the current leader
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
      _ <-
        // if one of the entries is a config entry and it is committed
        // we need to reload
        if response then
          request.entries.find(_.tag == LogEntryTag.Conf) match {
            case Some(configEntry) =>
              for {
                currentState <- state.get
                _ <-
                  if currentState.commitIndex.value >= configEntry.index then {
                    context.events.offer(
                      Event.NodeStateChanged(
                        oldState = currentState,
                        newState = currentState,
                        reason = StateTransitionReason.ConfigurationChanged
                      )
                    )
                  } else IO.unit
              } yield ()
            case None =>
              IO.unit
          }
        else IO.unit
    } yield response

  private def updateCommitIndex(leaderCommitIndex: Long, lastIndex: Long): IO[Unit] =
    state.update { currentState =>
      if leaderCommitIndex > currentState.commitIndex.value then
        currentState.copy(commitIndex = Index.of(math.min(leaderCommitIndex, lastIndex)))
      else
        currentState
    }

  override def addServer(request: AddServerRequest): IO[AddServerResponse] =
    for {
      currentState <- state.get
      response =
        AddServerResponse(
          status = AddServerResponse.Status.NotLeader,
          leaderHint = currentState.leader,
        )
    } yield response

  override def removeServer(request: RemoveServerRequest): IO[RemoveServerResponse] =
    for {
      currentState <- state.get
      response =
        RemoveServerResponse(
          status = RemoveServerResponse.Status.NotLeader,
          leaderHint = currentState.leader,
        )
    } yield response

}

object FollowerNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Follower],
    signal: Interrupt,
    trace: NodeTrace,
  ): IO[FollowerNode] = for {
    heartbeat <- Ref.of[IO, Option[Unit]](None)
    node = new FollowerNode(
      context,
      state,
      signal,
      heartbeat,
      trace,
    )
  } yield node
}

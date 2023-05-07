package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import com.google.protobuf.ByteString
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.*
import trabica.net.NodeApi

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val quorumId: String,
  val quorumPeer: Peer,
  val state: Ref[IO, NodeState.Leader],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val streamSignal: SignallingRef[IO, Boolean],
  val replicatedEntries: Ref[IO, Map[Index, Int]],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node[NodeState.Leader] {

  override final val logger = scribe.cats[IO]

  private final val id: Int = trace.leaderId

  override final val prefix: String = s"[leader-$id]"

  private final val heartbeatStreamRate: Long =
    context.config.getLong("trabica.leader.heartbeat-stream.rate")

  override def lens: NodeStateLens[NodeState.Leader] =
    NodeStateLens[NodeState.Leader]

  override def run: IO[FiberIO[Unit]] =
    for {
      h <- clients.use(heartbeatStream).supervise(supervisor)
      _ <- clients.use(replicate).supervise(supervisor)
    } yield h

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >>
      signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    IO.pure(false)

  def heartbeatStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream(clients)
      .interruptWhen(streamSignal)
      .filter(_.nonEmpty)
      .evalTap(_ => logger.debug(s"$prefix starting heartbeat stream"))
      .flatMap { cs =>
        Stream
          .fixedRateStartImmediately[IO](heartbeatStreamRate.milliseconds)
          .interruptWhen(streamSignal)
          .evalTap(_ => logger.trace(s"$prefix heartbeat stream wake up"))
          .evalMap { _ =>
            for {
              currentState <- state.get
              h            <- makeHeader(currentState)
              request = AppendEntriesRequest(
                header = h.some,
                prevLogIndex = currentState.commitIndex.value - 1,
                prevLogTerm = currentState.localState.currentTerm - 1,
              )
              responses <- cs.parTraverse { c =>
                logger.trace(s"$prefix sending heartbeat to ${c.show}") >>
                  c.appendEntries(request)
                    .timeout(100.milliseconds)
                    .attempt
                    .flatMap(r => onAppendEntriesResponse(c.quorumPeer, None, r))
              }
            } yield responses
          }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .onFinalize {
        logger.debug(s"$prefix heartbeat stream finalized")
      }
      .compile
      .drain

  private def replicate(clients: Vector[NodeApi]): IO[Unit] =
    Stream(clients)
      .map(Chunk.vector)
      .unchunks
      .parEvalMapUnorderedUnbounded(replicationStream)
      .compile
      .drain

  private def sendAppendEntriesRequest(
    client: NodeApi,
    currentState: NodeState.Leader,
    prevLogIndex: Long,
    prevLogTerm: Long,
    nextLogEntry: LogEntry,
  ): IO[Unit] = {
    val io = for {
      requestHeader <- makeHeader(currentState)
      request = AppendEntriesRequest(
        header = requestHeader.some,
        prevLogIndex = prevLogIndex,
        prevLogTerm = prevLogTerm,
        commitIndex = currentState.commitIndex.value,
        entries = Vector(nextLogEntry),
      )
      response <- client.appendEntries(request)
    } yield response
    io.timeout(100.milliseconds)
      .attempt
      .flatMap { response =>
        onAppendEntriesResponse(
          client.quorumPeer,
          Index.of(nextLogEntry.index).some,
          response
        )
      }
  }

  private def replicationStream(client: NodeApi): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2000.milliseconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.trace(s"$prefix starting replication to peer ${client.show}"))
      .evalMap { _ =>
        for {
          currentState <- state.get
          lastIndexOption = currentState.matchIndex.get(client.quorumPeer)
        } yield (currentState, lastIndexOption)
      }
      .flatMap { (currentState, lastIndexOption) =>
        context.store
          .streamFrom(lastIndexOption.getOrElse(Index.zero).increment)
          .interruptWhen(streamSignal)
          .evalTap { e =>
            logger.debug(
              s"$prefix replicating index:${e.index} -> ${client.show}",
              s"$prefix matchIndex: ${currentState.matchIndex}",
              s"$prefix commitIndex: ${currentState.commitIndex}",
            )
          }
          .evalMap { next =>
            for {
              prev <-
                context.store
                  .atIndex(Index.of(next.index - 1))
                  .recover(_ => LogEntry(0L, 0L, LogEntryTag.Data, ByteString.EMPTY))
              _ <- sendAppendEntriesRequest(client, currentState, prev.index, prev.term, next)
            } yield ()
          }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(
            s"$prefix error encountered in replication stream: ${e.getMessage}",
            e,
          )
        }
      }
      .onFinalize {
        logger.trace(s"$prefix replication stream finalized")
      }
      .compile
      .drain

  private def onAppendEntriesResponse(
    peer: Peer,
    index: Option[Index],
    response: Either[Throwable, AppendEntriesResponse]
  ): IO[Unit] =
    response match {
      case Left(e) =>
        logger.trace(s"$prefix no response from peer ${peer.show}, error: ${e.getMessage}")
      case Right(r) if !r.success =>
        for {
          _            <- logger.trace(s"$prefix response `${r.success}` from peer ${peer.show}")
          currentState <- state.get
          header       <- r.header.required(NodeError.InvalidMessage)
          _ <-
            if header.term > currentState.localState.currentTerm then {
              val newState = makeFollowerState(currentState, header.term)
              events.offer(
                Event.NodeStateChanged(
                  oldState = currentState,
                  newState = newState,
                  reason = StateTransitionReason.HigherTermDiscovered,
                )
              )
            } else IO.unit
        } yield ()

      case Right(_) =>
        index match {
          case Some(value) =>
            logger.debug(s"$prefix append entry success at index $value") >>
              onEntryAppended(peer, value)
          case None =>
            IO.unit
        }
    }

  private def onEntryAppended(peer: Peer, index: Index): IO[Unit] =
    for {
      peers <- quorumPeers
      majority = math.ceil(peers.length / 2) + 1
      re <- replicatedEntries.updateAndGet { re =>
        val count = re.get(index).map(_ + 1).getOrElse(1)
        re.updated(index, count)
      }
      _ <- state.update { s =>
        val matchIndex = s.matchIndex.updated(peer, index)
        val commitIndex =
          re.toVector.sortBy(_._1.value).foldLeft(s.commitIndex) { (acc, next) =>
            val (index, count) = next
            if count >= majority && index == acc.increment then
              acc.increment
            else acc
          }
        s.copy(matchIndex = matchIndex, commitIndex = commitIndex)
      }
      _ <- replicatedEntries.update { m =>
        if m.get(index) == peers.length.some then
          m.removed(index)
        else m
      }
    } yield ()

}

object LeaderNode {
  def instance(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    state: Ref[IO, NodeState.Leader],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[LeaderNode] = for {
    streamSignal      <- SignallingRef.of[IO, Boolean](false)
    replicatedEntries <- Ref.of[IO, Map[Index, Int]](Map.empty)
    node = new LeaderNode(
      context,
      quorumId,
      quorumPeer,
      state,
      events,
      signal,
      streamSignal,
      replicatedEntries,
      supervisor,
      trace,
    )
  } yield node
}

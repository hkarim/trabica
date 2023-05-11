package trabica.node

import cats.effect.*
import cats.syntax.all.*
import com.google.protobuf.ByteString
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.*
import trabica.net.NodeApi
import trabica.store.AppendResult

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Leader],
  val signal: Interrupt,
  val streamSignal: SignallingRef[IO, Boolean],
  val replicatedEntries: Ref[IO, Map[Index, Int]],
  val trace: NodeTrace,
) extends Node[NodeState.Leader] {

  override final val logger = scribe.cats[IO]

  private final val id: Int = trace.leaderId

  override final val prefix: String = s"[leader-$id]"

  private final val rpcTimeout: Long =
    context.config.getLong("trabica.rpc.timeout")

  private final val heartbeatStreamRate: Long =
    context.config.getLong("trabica.leader.heartbeat-stream.rate")

  private final val replicationStreamRate: Long =
    context.config.getLong("trabica.leader.replication-stream.rate")

  private final val addServerTimeout: Long =
    context.config.getLong("trabica.leader.add-server.timeout")

  override def lens: NodeStateLens[NodeState.Leader] =
    NodeStateLens[NodeState.Leader]

  override def run: IO[FiberIO[Unit]] =
    for {
      h <- clients.use(heartbeatStream).supervise(context.supervisor)
      _ <- clients.use(replicate).supervise(context.supervisor)
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
                    .timeout(rpcTimeout.milliseconds)
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
    Stream
      .iterable(clients)
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
    io.timeout(rpcTimeout.milliseconds)
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
      .fixedRateStartImmediately[IO](replicationStreamRate.milliseconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.trace(s"$prefix starting replication to peer ${client.show}"))
      .evalMap { _ =>
        for {
          currentState <- state.get
          lastIndexOption = currentState.matchIndex.get(client.quorumPeer)
        } yield lastIndexOption
      }
      .flatMap { lastIndexOption =>
        context.store
          .streamFrom(lastIndexOption.getOrElse(Index.zero).increment)
          .interruptWhen(streamSignal)
          .evalTap { e =>
            state.get.flatTap { currentState =>
              logger.debug(
                s"$prefix replicating index:${e.index} -> ${client.show}",
                s"$prefix matchIndex: ${currentState.matchIndex.show}",
                s"$prefix commitIndex: ${currentState.commitIndex}",
                s"$prefix currentTerm: ${currentState.localState.currentTerm}",
              )
            }
          }
          .evalMap { next =>
            for {
              currentState <- state.get
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
          header       <- r.header.required
          _ <-
            if header.term > currentState.localState.currentTerm then {
              val newState = makeFollowerState(currentState, header.term, header.node)
              context.events.offer(
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
            onEntryAppended(peer, value)
          case None =>
            IO.unit
        }
    }

  private def onEntryAppended(peer: Peer, index: Index): IO[Unit] =
    for {
      peers <- quorumPeers
      peersLength = peers.length + 1 // counting ourselves
      majority = math.ceil(peersLength / 2) + 1
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
      currentState <- state.get
      _ <- logger.debug(
        s"$prefix append entry success at index $index",
        s"$prefix matchIndex: ${currentState.matchIndex.show}",
        s"$prefix commitIndex: ${currentState.commitIndex}",
      )
    } yield ()

  override def addServer(request: AddServerRequest): IO[AddServerResponse] =
    for {
      peers <- quorumPeers
      node <- request.node
        .required(NodeError.ValueError(s"$prefix missing node in add server request"))
      peer <- node.peer
        .required(NodeError.ValueError(s"$prefix missing peer in add server request"))
      response <-
        if peers.contains(peer) || peer.some == quorumNode.peer then
          logger.debug(s"$prefix peer ${peer.show} already exists") >>
            AddServerResponse(
              status = AddServerResponse.Status.OK,
              leaderHint = quorumNode.some,
            ).pure[IO]
        else
          logger.debug(s"$prefix peer ${peer.show} doesn't exist, attempt to add it") >>
            tryAddServer(node, peer)
    } yield response

  private def tryAddServer(node: QuorumNode, peer: Peer): IO[AddServerResponse] = {
    def stream(client: NodeApi, fromIndex: Index): IO[Index] =
      context.store
        .streamFrom(fromIndex)
        .interruptWhen(signal)
        .handleErrorWith(_ => Stream.empty)
        .evalMap { entry =>
          for {
            currentState <- state.get
            prev <- context.store
              .atIndex(Index.of(entry.index - 1))
              .recover(_ => LogEntry.defaultInstance)
            h <- makeHeader(currentState)
            request = AppendEntriesRequest(
              header = h.some,
              prevLogIndex = prev.index,
              prevLogTerm = prev.term,
              commitIndex = currentState.commitIndex.value,
              entries = Vector(entry),
            )
            responseSuccess <-
              client.appendEntries(request)
                .map(_.success)
                .timeout(rpcTimeout.milliseconds)
                .recover(_ => false)
            replicatedIndex = if responseSuccess then Index.of(entry.index) else fromIndex
          } yield replicatedIndex
        }
        .compile
        .fold(fromIndex) { (_, next) =>
          next
        }

    def replicate(fromIndex: Index): IO[Index] =
      Stream
        .resource(
          context.networking
            .client(prefix, node.id, peer)
        )
        .interruptWhen(signal)
        .evalMap { client =>
          stream(client, fromIndex)
        }
        .compile
        .fold(fromIndex) { (_, next) =>
          next
        }

    def rounds(desiredIndex: Index): IO[Index] = {
      val roundOne = replicate(Index.one)
      (2 to 3).toVector.foldLeft(roundOne) { (io, _) =>
        io.flatMap { index =>
          if index == desiredIndex then
            index.pure[IO]
          else
            replicate(index)
        }
      }
    }

    def ensureLastConfigurationCommitted: IO[Unit] =
      for {
        currentState      <- state.get
        configEntryOption <- context.store.configuration
        _ <- configEntryOption match {
          case Some(configEntry) =>
            if currentState.commitIndex.value >= configEntry.index then
              IO.unit
            else
              IO.sleep(1.seconds) >> ensureLastConfigurationCommitted
          case None =>
            IO.unit
        }
      } yield ()

    def ensureConfigurationCommitted(
      prevLogIndex: Long,
      prevLogTerm: Long,
      nextLogEntry: LogEntry
    ): IO[Unit] =
      for {
        currentState      <- state.get
        configEntryOption <- context.store.configuration
        _ <- configEntryOption match {
          case Some(configEntry) =>
            if currentState.commitIndex.value >= configEntry.index then
              IO.unit
            else
              for {
                _ <-
                  clients.use { cs =>
                    cs.parTraverse { c =>
                      sendAppendEntriesRequest(
                        c,
                        currentState,
                        prevLogIndex,
                        prevLogTerm,
                        nextLogEntry
                      )
                    }
                  }
                _ <- ensureConfigurationCommitted(
                  prevLogIndex,
                  prevLogTerm,
                  nextLogEntry
                )

              } yield ()

          case None =>
            IO.unit
        }
      } yield ()

    def commit: IO[Unit] =
      for {
        currentState <- state.get
        lastOption   <- context.store.last
        last = lastOption.getOrElse(LogEntry.defaultInstance)
        q <- quorum
        newQuorum = q.copy(nodes = q.nodes.appended(node).toVector.distinct)
        entry = LogEntry(
          index = last.index + 1L,
          term = currentState.localState.currentTerm,
          tag = LogEntryTag.Conf,
          data = newQuorum.toByteString,
        )
        r <- context.store.append(entry)
        _ <-
          if r == AppendResult.Appended then
            ensureConfigurationCommitted(last.index, last.term, entry)
          else
            commit
      } yield ()

    val run = for {
      currentState <- state.get
      _ <- logger.debug(
        s"$prefix [add-server::initial-state] commitIndex: ${currentState.commitIndex}",
        s"$prefix [add-server::initial-state] matchIndex: ${currentState.matchIndex.show}",
      )
      replicated <-
        rounds(currentState.commitIndex)
      _ <- logger.debug(
        s"$prefix [add-server::after-rounds] replicatedIndex: $replicated",
        s"$prefix [add-server::after-rounds] commitIndex: ${currentState.commitIndex}",
        s"$prefix [add-server::after-rounds] matchIndex: ${currentState.matchIndex.show}",
      )
      response <-
        if replicated == currentState.commitIndex then
          for {
            _ <- logger.debug(s"$prefix [add-server] invoking ensureLastConfigurationCommitted")
            _ <- ensureLastConfigurationCommitted
            _ <- logger.debug(s"$prefix [add-server] invoking commit")
            _ <- commit
            _ <- logger.debug(s"$prefix [add-server] returning response")
            response = AddServerResponse(
              status = AddServerResponse.Status.OK,
              leaderHint = quorumNode.some,
            )
          } yield response
        else
          logger.debug(s"$prefix [add-server] timeout") >>
            AddServerResponse(
              status = AddServerResponse.Status.Timeout,
              leaderHint = quorumNode.some,
            ).pure[IO]
    } yield response

    run
      .timeout(addServerTimeout.milliseconds)
      .flatMap { response =>
        if response.status == AddServerResponse.Status.OK then
          for {
            currentState <- state.get
            _ <- context.events.offer(
              Event.NodeStateChanged(
                oldState = currentState,
                newState = currentState,
                reason = StateTransitionReason.ConfigurationChanged,
              )
            )
          } yield response
        else response.pure[IO]
      }
      .recover { _ =>
        AddServerResponse(
          status = AddServerResponse.Status.Timeout,
          leaderHint = quorumNode.some,
        )
      }
  }

}

object LeaderNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Leader],
    signal: Interrupt,
    trace: NodeTrace,
  ): IO[LeaderNode] = for {
    streamSignal      <- SignallingRef.of[IO, Boolean](false)
    replicatedEntries <- Ref.of[IO, Map[Index, Int]](Map.empty)
    node = new LeaderNode(
      context,
      state,
      signal,
      streamSignal,
      replicatedEntries,
      trace,
    )
  } yield node
}

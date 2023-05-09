package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.SignallingRef
import scribe.Scribe
import trabica.model.*
import trabica.net.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

class CandidateNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Candidate],
  val signal: Interrupt,
  val streamSignal: SignallingRef[IO, Boolean],
  val trace: NodeTrace,
) extends Node[NodeState.Candidate] {

  override final val logger: Scribe[IO] = scribe.cats[IO]

  private final val id: Int = trace.candidateId

  override final val prefix: String = s"[candidate-$id]"

  private final val voteStreamRate: Long =
    context.config.getLong("trabica.candidate.vote-stream.rate")

  private final val voteStreamTimeoutMin: Long =
    context.config.getLong("trabica.candidate.vote-stream.timeout.min")

  private final val voteStreamTimeoutMax: Long =
    context.config.getLong("trabica.candidate.vote-stream.timeout.max")

  private final val voteRequestTimeout: Long =
    context.config.getLong("trabica.candidate.vote-request.timeout")

  override def lens: NodeStateLens[NodeState.Candidate] =
    NodeStateLens[NodeState.Candidate]

  override def run: IO[FiberIO[Unit]] =
    clients.use(voteStream).supervise(context.supervisor)

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >>
      signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  private def timeout: IO[Unit] =
    for {
      _ <- logger.debug(s"$prefix vote stream timed out, restarting election")
      newState <- state.updateAndGet { s =>
        val localState = s.localState.copy(currentTerm = s.localState.currentTerm + 1)
        s.copy(localState = localState)
      }
      _ <- logger.debug(s"$prefix starting vote stream with term ${newState.localState.currentTerm}")
      _ <- context.events.offer(
        Event.NodeStateChanged(newState, newState, StateTransitionReason.ElectionStarted)
      )
    } yield ()

  private def voteStream(nodes: Vector[NodeApi]): IO[Unit] =
    Random.scalaUtilRandom[IO]
      .flatMap { r =>
        r.betweenLong(voteStreamTimeoutMin, voteStreamTimeoutMax)
      }
      .flatMap { t =>
        Stream
          .fixedRateStartImmediately[IO](voteStreamRate.milliseconds)
          .interruptWhen(streamSignal)
          .evalTap(_ => logger.debug(s"$prefix vote stream wake up"))
          .evalMap { _ =>
            for {
              _     <- logger.debug(s"$prefix requesting vote from ${nodes.length} client(s)")
              entry <- context.store.last
              h     <- makeHeader
              request = VoteRequest(
                header = h.some,
                lastLogIndex = entry.map(_.index).getOrElse(0L),
                lastLogTerm = entry.map(_.term).getOrElse(0L),
              )
              responses <- nodes.parTraverse { n =>
                n.vote(request)
                  .timeout(voteRequestTimeout.milliseconds)
                  .attempt
                  .flatMap(r => onVote(n, r))
              }
            } yield responses
          }
          .handleErrorWith { e =>
            Stream.eval {
              logger.error(s"$prefix error encountered in vote stream: ${e.getMessage}", e)
            }
          }
          .onFinalize {
            logger.debug(s"$prefix vote stream finalized")
          }
          .compile
          .drain
          .timeout(t.milliseconds)
          .attempt
          .flatMap {
            case Left(_: TimeoutException) =>
              timeout.void
            case Left(e) =>
              logger.error(s"$prefix vote stream error", e)
            case Right(_) =>
              IO.unit
          }
      }

  private def onVote(client: NodeApi, response: Either[Throwable, VoteResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        val peer = client.quorumPeer
        logger.debug(
          s"$prefix vote response error from peer ${peer.show}",
          s"${e.getMessage}, ignoring"
        )
      case Right(VoteResponse(Some(_), true, _)) =>
        val peer = client.quorumPeer
        for {
          _     <- logger.debug(s"$prefix vote granted from peer ${peer.show}")
          peers <- quorumPeers
          peersLength = peers.length + 1 // counting ourselves
          _ <- state.flatModify { currentState =>
            val votes            = (currentState.votes + client.quorumNode).toVector
            val currentlyElected = currentState.elected
            val newlyElected     = votes.length >= math.ceil(peersLength / 2) + 1
            // the purpose here is to prevent firing state change events once we're already elected
            // while the node is still in transition
            // thus preventing unnecessary node transition more than once
            val io = for {
              _ <- logger.debug(s"$prefix votes reached ${votes.length} of total $peersLength")
              matchIndex <- quorum.map { q =>
                q.nodes
                  .toVector
                  .filterNot(_.id == context.quorumId)
                  .foldLeft(Map.empty[Peer, Index]) { (m, next) =>
                    next.peer match {
                      case Some(p) =>
                        m.updated(p, Index.zero)
                      case None =>
                        m
                    }
                  }
              }.recover(_ => Map.empty[Peer, Index])

              newState = NodeState.Leader(
                localState = LocalState(
                  node = quorumNode.some,
                  currentTerm = currentState.localState.currentTerm,
                  votedFor = None,
                ),
                commitIndex = currentState.commitIndex,
                lastApplied = currentState.lastApplied,
                nextIndex = matchIndex.map((k, v) => (k, v.increment)),
                matchIndex = matchIndex,
              )

              _ <-
                if newlyElected && !currentlyElected then {
                  logger.debug(s"$prefix elected, votes reached majority ${votes.length} of total $peersLength") >>
                    context.events.offer(
                      Event.NodeStateChanged(
                        currentState,
                        newState,
                        StateTransitionReason.ElectedLeader
                      )
                    )
                } else IO.unit

            } yield ()

            (currentState.copy(votes = votes.toSet, elected = newlyElected), io)
          }
        } yield ()
      case Right(VoteResponse(Some(header), false, _)) =>
        val peer = client.quorumPeer
        for {
          _            <- logger.debug(s"$prefix vote denied from peer ${peer.show}")
          currentState <- state.get
          _ <-
            if header.term > currentState.localState.currentTerm then {
              val followerState = makeFollowerState(currentState, header.term)
              context.events.offer(
                Event.NodeStateChanged(
                  oldState = currentState,
                  newState = followerState,
                  reason = StateTransitionReason.HigherTermDiscovered,
                )
              )
            } else IO.unit
        } yield ()
      case Right(v) =>
        logger.debug(s"$prefix invalid vote message received: $v") >>
          IO.raiseError(NodeError.InvalidMessage)
    }

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required(NodeError.InvalidMessage)
      _ <-
        if currentState.localState.currentTerm <= header.term then {
          // a leader has been elected other than this candidate, change state to follower
          val newState = makeFollowerState(currentState, header.term)
          context.events.offer(
            Event.NodeStateChanged(
              oldState = currentState,
              newState = newState,
              reason = StateTransitionReason.HigherTermDiscovered
            )
          )
        } else IO.unit
    } yield false

}

object CandidateNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Candidate],
    signal: Interrupt,
    trace: NodeTrace,
  ): IO[CandidateNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new CandidateNode(
      context,
      state,
      signal,
      streamSignal,
      trace,
    )
  } yield node
}

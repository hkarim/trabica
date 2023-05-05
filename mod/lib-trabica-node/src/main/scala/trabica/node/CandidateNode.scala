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
  val quorumId: String,
  val quorumPeer: Peer,
  val state: Ref[IO, NodeState.Candidate],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val streamSignal: SignallingRef[IO, Boolean],
  val supervisor: Supervisor[IO],
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

  def lens: NodeStateLens[NodeState.Candidate] =
    NodeStateLens[NodeState.Candidate]

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >>
      signal.complete(Right(())).void >>
      logger.debug(s"$prefix interrupted")

  private def timeout: IO[Unit] =
    for {
      _ <- logger.debug(s"$prefix vote stream timed out, restarting election")
      s <- state.updateAndGet(s => s.copy(votingTerm = s.votingTerm.increment))
      _ <- logger.debug(s"$prefix starting vote stream with term ${s.votingTerm}")
      _ <- events.offer(
        Event.NodeStateChanged(s, s, StateTransitionReason.ConfigurationChanged)
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
              _ <- logger.debug(s"$prefix requesting vote from ${nodes.length} client(s)")
              h <- header
              request = VoteRequest(header = h.some)
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
          s"$prefix vote response error from peer ${peer.host}:${peer.port}",
          s"${e.getMessage}, ignoring"
        )
      case Right(VoteResponse(Some(_), true, _)) =>
        val peer = client.quorumPeer
        for {
          _     <- logger.debug(s"$prefix vote granted from peer ${peer.host}:${peer.port}")
          peers <- quorumPeers
          _ <- state.flatModify { currentState =>
            val votes            = currentState.votes + client.quorumNode
            val currentlyElected = currentState.elected
            val newlyElected     = votes.size >= math.ceil(peers.size / 2) + 1
            // the purpose here is to prevent firing state change events once we're already elected
            // while the node is still in transition
            // thus preventing unnecessary node transition more than once
            val io =
              if newlyElected && !currentlyElected then {
                val newState = NodeState.Leader(
                  localState = LocalState(
                    node = QuorumNode(id = quorumId, peer = quorumPeer.some).some,
                    currentTerm = currentState.votingTerm.value,
                    votedFor = None,
                  ),
                  commitIndex = currentState.commitIndex,
                  lastApplied = currentState.lastApplied,
                  nextIndex = Map.empty,
                  matchIndex = Map.empty,
                )
                events.offer(
                  Event.NodeStateChanged(currentState, newState, StateTransitionReason.ElectedLeader)
                )
              } else IO.unit
            (currentState.copy(votes = votes, elected = newlyElected), io)
          }
        } yield ()
      case Right(VoteResponse(Some(_), false, _)) =>
        val peer = client.quorumPeer
        logger.debug(s"$prefix vote denied from peer ${peer.host}:${peer.port}")
      case Right(v) =>
        logger.debug(s"$prefix invalid vote message received: $v") >>
          IO.raiseError(NodeError.InvalidMessage)
    }

  def run: IO[FiberIO[Unit]] =
    clients.use(voteStream).supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required(NodeError.InvalidMessage)
      node         <- header.node.required(NodeError.InvalidMessage)
      peer         <- node.peer.required(NodeError.InvalidMessage)
      _ <-
        if currentState.votingTerm.value == header.term then {
          // a leader has been elected other than this candidate, change state to follower
          val newState = NodeState.Follower(
            localState = LocalState(
              node = QuorumNode(id = quorumId, peer = Some(quorumPeer)).some,
              currentTerm = header.term,
              votedFor = None,
            ),
            commitIndex = currentState.commitIndex,
            lastApplied = currentState.lastApplied
          )
          events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.HigherTermDiscovered))
        } else Node.termCheck(header, currentState, events)
    } yield false

  override def vote(request: VoteRequest): IO[Boolean] =
    IO.pure(false)
}

object CandidateNode {
  def instance(
    context: NodeContext,
    quorumId: String,
    quorumPeer: Peer,
    state: Ref[IO, NodeState.Candidate],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[CandidateNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new CandidateNode(
      context,
      quorumId,
      quorumPeer,
      state,
      events,
      signal,
      streamSignal,
      supervisor,
      trace,
    )
  } yield node
}

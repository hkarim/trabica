package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import trabica.model.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

class CandidateNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Candidate],
  val events: Queue[IO, Event],
  val signal: Interrupt,
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.candidateId

  private final val prefix: String = s"[candidate-$id]"

  private final val voteStreamRate: Long =
    context.config.getLong("trabica.candidate.vote-stream.rate")

  private final val voteStreamTimeoutMin: Long =
    context.config.getLong("trabica.candidate.vote-stream.timeout.min")

  private final val voteStreamTimeoutMax: Long =
    context.config.getLong("trabica.candidate.vote-stream.timeout.max")

  private final val voteRequestTimeout: Long =
    context.config.getLong("trabica.candidate.vote-request.timeout")

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

  private def peersChanged(newState: NodeState.Candidate): IO[Unit] =
    for {
      _            <- logger.debug(s"$prefix peers changed, restarting vote stream")
      currentState <- state.get
      _            <- events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.ConfigurationChanged))
    } yield ()

  private def timeout: IO[Unit] =
    for {
      _ <- logger.debug(s"$prefix vote stream timed out, restarting election")
      s <- state.updateAndGet(s => s.copy(currentTerm = s.currentTerm.increment))
      _ <- logger.debug(s"$prefix starting vote stream with term ${s.currentTerm}")
      _ <- events.offer(Event.NodeStateChanged(s, s, StateTransitionReason.ConfigurationChanged))
    } yield ()

  private def voteStream(nodes: Vector[NodeApi]): IO[Unit] =
    Random.scalaUtilRandom[IO]
      .flatMap { r =>
        r.betweenLong(voteStreamTimeoutMin, voteStreamTimeoutMax)
      }
      .flatMap { t =>
        Stream
          .fixedRateStartImmediately[IO](voteStreamRate.milliseconds)
          .interruptWhen(signal)
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
                  .flatMap(onVote)
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

  private def onVote(response: Either[Throwable, VoteResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"$prefix vote response error ${e.getMessage}, ignoring")
      case Right(VoteResponse(Some(header), true, _)) =>
        for {
          peer         <- header.peer.required
          _            <- logger.debug(s"$prefix vote granted from peer ${peer.host}:${peer.port}")
          currentState <- state.updateAndGet(s => s.copy(votes = s.votes + peer))
          peers = currentState.peers
          _ <-
            if currentState.votes.size >= math.ceil(peers.size / 2) + 1 then {
              val newState = NodeState.Leader(
                self = currentState.self,
                peers = currentState.peers,
                currentTerm = currentState.currentTerm,
                votedFor = None,
                commitIndex = currentState.commitIndex,
                lastApplied = currentState.lastApplied,
                nextIndex = Map.empty,
                matchIndex = Map.empty,
              )
              events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.ElectedLeader))
            } else IO.unit
        } yield ()
      case Right(VoteResponse(Some(header), false, _)) =>
        for {
          peer <- header.peer.required
          _    <- logger.debug(s"$prefix vote denied from peer ${peer.host}:${peer.port}")
        } yield ()
      case Right(v) =>
        logger.debug(s"$prefix invalid vote message received: $v") >>
          IO.raiseError(NodeError.InvalidMessage)
    }

  def run: IO[FiberIO[Unit]] =
    clients.use(voteStream).supervise(supervisor)

  override def appendEntries(request: AppendEntriesRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      peer         <- header.peer.required
      _ <-
        if currentState.currentTerm.value == header.term then {
          // a leader has been elected other than this candidate, change state to follower
          val newState = NodeState.Follower(
            self = currentState.self,
            peers = currentState.peers + peer,
            leader = peer,
            currentTerm = Term.of(header.term),
            votedFor = None,
            commitIndex = currentState.commitIndex,
            lastApplied = currentState.lastApplied
          )
          events.offer(Event.NodeStateChanged(currentState, newState, StateTransitionReason.HigherTermDiscovered))
        } else Node.termCheck(header, currentState, events)
    } yield false

  override def join(request: JoinRequest): IO[JoinResponse.Status] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} requested to join")
      currentState <- state.get
      response = JoinResponse.Status.UnknownLeader(
        JoinResponse.UnknownLeader(knownPeers = currentState.peers.toSeq)
      )
      newState = currentState.copy(peers = currentState.peers + peer)
      _ <- peersChanged(newState)
    } yield response

  override def vote(request: VoteRequest): IO[Boolean] =
    IO.pure(false)
}

object CandidateNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Candidate],
    events: Queue[IO, Event],
    signal: Interrupt,
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[CandidateNode] = IO.pure {
    new CandidateNode(
      context,
      state,
      events,
      signal,
      supervisor,
      trace,
    )
  }
}

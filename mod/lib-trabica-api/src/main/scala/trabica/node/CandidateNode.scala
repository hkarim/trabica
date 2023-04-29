package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.{Event, NodeError, NodeState}
import trabica.rpc.*

import scala.concurrent.duration.*

class CandidateNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Candidate],
  val events: Queue[IO, Event],
  val signal: Deferred[IO, Unit],
  val streamSignal: SignallingRef[IO, Boolean],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.candidateId

  private final val prefix: String = s"[candidate-$id]"

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def clients: Resource[IO, Vector[NodeApi]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        NodeApi.client(prefix, peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Candidate): IO[FiberIO[Unit]] =
    for {
      _ <- logger.debug(s"$prefix peers changed, restarting vote stream")
      _ <- streamSignal.set(true) // stop the stream
      _ <- state.set(newState)
      _ <- streamSignal.set(false)                       // reset the signal
      f <- clients.use(voteStream).supervise(supervisor) // start the stream
    } yield f

  private def voteStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.debug(s"$prefix vote stream wake up"))
      .flatMap { _ =>
        Stream.eval {
          for {
            _ <- logger.debug(s"$prefix requesting vote from ${clients.length} client(s)")
            h <- header
            request = VoteRequest(header = h.some)
            responses <- clients.parTraverse { c =>
              c.vote(request)
                .timeout(100.milliseconds)
                .attempt
                .flatMap(onVote)
            }
          } yield responses
        }
      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in vote stream: ${e.getMessage}", e)
        }
      }
      .onFinalize {
        logger.debug(s"$prefix vote stream stopped")
      }
      .compile
      .drain

  private def onVote(response: Either[Throwable, VoteResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"$prefix vote response error ${e.getMessage}, ignoring")
      case Right(VoteResponse(Some(header), true, _)) =>
        for {
          peer <- header.peer.required
          _    <- logger.debug(s"$prefix vote granted from peer ${peer.host}:${peer.port}")
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
      _            <- Node.termCheck(header, currentState, events)
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
    for {
      currentState <- state.get
      header       <- request.header.required
      voteGranted = currentState.votedFor.isEmpty && header.term > currentState.currentTerm
      _ <-
        if voteGranted then {
          for {
            peer <- header.peer.required
            _    <- state.set(currentState.copy(votedFor = peer.some))
          } yield ()
        } else IO.unit
    } yield voteGranted

}

object CandidateNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Candidate],
    events: Queue[IO, Event],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[CandidateNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new CandidateNode(
      context,
      state,
      events,
      signal,
      streamSignal,
      supervisor,
      trace,
    )
  } yield node
}

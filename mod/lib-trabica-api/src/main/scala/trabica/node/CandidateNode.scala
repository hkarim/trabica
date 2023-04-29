package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.{Event, NodeError, NodeState}
import trabica.net.GrpcClient
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

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(peer)
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

  private def voteStream(clients: Vector[TrabicaFs2Grpc[IO, Metadata]]): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .interruptWhen(streamSignal)
      .evalTap(_ => logger.debug(s"$prefix vote stream wake up"))
      .flatMap { _ =>
        Stream.eval {
          for {
            _         <- logger.debug(s"$prefix requesting vote from ${clients.length} client(s)")
            messageId <- context.messageId.getAndUpdate(_.increment)
            s         <- state.get
            request = VoteRequest(
              header = Header(
                peer = s.self.some,
                messageId = messageId.value,
                term = s.currentTerm,
              ).some
            )
            responses <- clients.parTraverse { c =>
              c.vote(request, new Metadata)
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

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = AppendEntriesResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
      )
      header <- request.header.required
      _      <- Node.termCheck(header, currentState, events)
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      peerHeader   <- request.header.required
      peer         <- peerHeader.peer.required
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} requested to join")
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = JoinResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
        status = JoinResponse.Status.UnknownLeader(
          JoinResponse.UnknownLeader(knownPeers = currentState.peers.toSeq)
        )
      )
      newState = currentState.copy(peers = currentState.peers + peer)
      _ <- peersChanged(newState)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      header       <- request.header.required
      voteGranted = currentState.votedFor.isEmpty && header.term > currentState.currentTerm
      response = VoteResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
        voteGranted = voteGranted
      )
      _ <-
        if voteGranted then {
          for {
            peer <- header.peer.required
            _    <- state.set(currentState.copy(votedFor = peer.some))
          } yield ()
        } else IO.unit
    } yield response

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

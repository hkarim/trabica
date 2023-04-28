package trabica.fsm

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.context.NodeContext
import trabica.model.{Event, NodeState}
import trabica.net.GrpcClient
import trabica.rpc.*

import scala.concurrent.duration.*

class LeaderNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState.Leader],
  val events: Queue[IO, Event],
  val signal: Deferred[IO, Unit],
  val streamSignal: SignallingRef[IO, Boolean],
  val supervisor: Supervisor[IO],
  val trace: NodeTrace,
) extends Node {

  private final val logger = scribe.cats[IO]

  private final val id: Int = trace.leaderId

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"[leader-$id] interrupted")

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      currentState <- Resource.eval(state.get)
      clients <- currentState.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Leader): IO[FiberIO[Unit]] =
    for {
      _ <- logger.debug(s"[leader-$id] peers changed, restarting heartbeat stream")
      _ <- streamSignal.set(true) // stop the stream
      _ <- state.set(newState)
      _ <- streamSignal.set(false)                            // reset the signal
      f <- clients.use(heartbeatStream).supervise(supervisor) // start the stream
    } yield f

  private def heartbeatStream(clients: Vector[TrabicaFs2Grpc[IO, Metadata]]): IO[Unit] =
    Stream(clients)
      .interruptWhen(streamSignal)
      .filter(_.nonEmpty)
      .evalTap(_ => logger.debug(s"[leader-$id] starting heartbeat stream"))
      .flatMap { cs =>
        Stream
          .fixedRateStartImmediately[IO](2.seconds)
          .evalTap(_ => logger.debug(s"[leader-$id] heartbeat stream wake up"))
          .evalMap { _ =>
            for {
              messageId    <- context.messageId.getAndUpdate(_.increment)
              currentState <- state.get
              request = AppendEntriesRequest(
                header = Header(
                  peer = currentState.self.some,
                  messageId = messageId.value,
                  term = currentState.currentTerm,
                ).some,
                peers = currentState.peers.toSeq,
              )
              responses <- cs.parTraverse { c =>
                c.appendEntries(request, new Metadata)
                  .timeout(100.milliseconds)
                  .attempt
                  .flatMap(onResponse)
              }
            } yield responses
          }

      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"[leader-$id] error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def onResponse(response: Either[Throwable, AppendEntriesResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"[leader-$id] no response ${e.getMessage}")
      case Right(r) =>
        for {
          header       <- r.header.required
          currentState <- state.get
          _ <-
            if header.term > currentState.currentTerm then {
              for {
                peer <- header.peer.required
                newState = NodeState.Follower(
                  id = currentState.id,
                  self = currentState.self,
                  peers = Set(peer),
                  leader = peer,
                  currentTerm = header.term,
                  votedFor = None,
                  commitIndex = currentState.commitIndex,
                  lastApplied = currentState.lastApplied,
                )
                _ <- events.offer(Event.NodeStateChanged(newState))
              } yield ()
            } else
              IO.unit
        } yield ()
    }

  def run: IO[FiberIO[Unit]] =
    clients.use(heartbeatStream).supervise(supervisor)

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
      _            <- logger.debug(s"[leader-$id] peer ${peer.host}:${peer.port} joining")
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      response = JoinResponse(
        header = Header(
          peer = currentState.self.some,
          messageId = messageId.value,
          term = currentState.currentTerm,
        ).some,
        status = JoinResponse.Status.Accepted(
          JoinResponse.Accepted()
        )
      )
      newState = currentState.copy(peers = currentState.peers + peer) // change the peers
      _ <- peersChanged(newState)
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      messageId    <- context.messageId.getAndUpdate(_.increment)
      currentState <- state.get
      header       <- request.header.required
      _ <- logger.debug(
        s"[leader-$id] vote requested. votedFor=${currentState.votedFor}, term=${currentState.currentTerm}, request.term=${header.term}"
      )
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

object LeaderNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState.Leader],
    events: Queue[IO, Event],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO],
    trace: NodeTrace,
  ): IO[LeaderNode] = for {
    streamSignal <- SignallingRef.of[IO, Boolean](false)
    node = new LeaderNode(
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

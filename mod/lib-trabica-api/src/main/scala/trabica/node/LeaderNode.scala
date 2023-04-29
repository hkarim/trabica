package trabica.node

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.SignallingRef
import trabica.model.{Event, NodeState}
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

  private final val prefix: String = s"[leader-$id]"

  override def interrupt: IO[Unit] =
    streamSignal.set(true) >> signal.complete(()).void >>
      logger.debug(s"$prefix interrupted")

  override def stateIO: IO[NodeState] = state.get

  private def clients: Resource[IO, Vector[NodeApi]] =
    for {
      currentState <- Resource.eval(state.get)
      clients <- currentState.peers.toVector.traverse { peer =>
        NodeApi.client(prefix, peer)
      }
    } yield clients

  private def peersChanged(newState: NodeState.Leader): IO[FiberIO[Unit]] =
    for {
      _ <- logger.debug(s"$prefix peers changed, restarting heartbeat stream")
      _ <- streamSignal.set(true) // stop the stream
      _ <- state.set(newState)
      _ <- streamSignal.set(false)                            // reset the signal
      f <- clients.use(heartbeatStream).supervise(supervisor) // start the stream
    } yield f

  private def heartbeatStream(clients: Vector[NodeApi]): IO[Unit] =
    Stream(clients)
      .interruptWhen(streamSignal)
      .filter(_.nonEmpty)
      .evalTap(_ => logger.debug(s"$prefix starting heartbeat stream"))
      .flatMap { cs =>
        Stream
          .fixedRateStartImmediately[IO](2.seconds)
          .evalTap(_ => logger.debug(s"$prefix heartbeat stream wake up"))
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
                c.appendEntries(request)
                  .timeout(100.milliseconds)
                  .attempt
                  .flatMap(onResponse)
              }
            } yield responses
          }

      }
      .handleErrorWith { e =>
        Stream.eval {
          logger.error(s"$prefix error encountered in heartbeat stream: ${e.getMessage}", e)
        }
      }
      .compile
      .drain

  private def onResponse(response: Either[Throwable, AppendEntriesResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"$prefix no response ${e.getMessage}")
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
      _            <- logger.debug(s"$prefix peer ${peer.host}:${peer.port} joining")
      currentState <- state.get
      status = JoinResponse.Status.Accepted(
        JoinResponse.Accepted()
      )
      newState = currentState.copy(peers = currentState.peers + peer) // change the peers
      _ <- peersChanged(newState)
    } yield status

  override def vote(request: VoteRequest): IO[Boolean] =
    for {
      currentState <- state.get
      header       <- request.header.required
      _ <- logger.debug(
        s"$prefix vote requested. votedFor=${currentState.votedFor}, term=${currentState.currentTerm}, request.term=${header.term}"
      )
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
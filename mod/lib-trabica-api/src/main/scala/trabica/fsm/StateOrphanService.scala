package trabica.fsm

import cats.effect.*
import cats.effect.std.Queue
import cats.syntax.all.*
import io.grpc.Metadata
import trabica.context.NodeContext
import trabica.model.NodeState
import trabica.rpc.*

class StateOrphanService(context: NodeContext, state: Ref[IO, NodeState], events: Queue[IO, NodeState])
  extends TrabicaFs2Grpc[IO, Metadata] {
  private final val logger = scribe.cats[IO]

  logger.debug(s"$context$events")

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      s         <- state.orphan
      response = AppendEntriesResponse(
        header = Header(
          peer = s.self.some,
          messageId = messageId.value,
          term = s.currentTerm,
        ).some,
      )
    } yield response

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      s         <- state.orphan
      response = VoteResponse(
        header = Header(
          peer = s.self.some,
          messageId = messageId.value,
          term = s.currentTerm,
        ).some,
      )
    } yield response

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      s         <- state.orphan
      response = JoinResponse(
        header = Header(
          peer = s.self.some,
          messageId = messageId.value,
          term = s.currentTerm,
        ).some,
        status = JoinResponse.Status.UnknownLeader(
          JoinResponse.UnknownLeader(knownPeers = s.peers.toSeq)
        )
      )
    } yield response
}

object StateOrphanService {
  def instance(context: NodeContext, state: Ref[IO, NodeState], events: Queue[IO, NodeState]): StateOrphanService =
    new StateOrphanService(context, state, events)
}

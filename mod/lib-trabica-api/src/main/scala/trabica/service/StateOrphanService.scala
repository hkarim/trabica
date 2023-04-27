package trabica.service

import cats.effect.*
import cats.effect.std.Queue
import io.grpc.Metadata
import trabica.context.NodeContext
import trabica.model.NodeState
import trabica.rpc.*

class StateOrphanService(context: NodeContext, events: Queue[IO, NodeState]) extends TrabicaFs2Grpc[IO, Metadata] {
  private final val logger = scribe.cats[IO]

  logger.debug(s"$context$events")

  override def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] =
    ???

  override def vote(request: VoteRequest, metadata: Metadata): IO[VoteResponse] =
    ???

  override def join(request: JoinRequest, metadata: Metadata): IO[JoinResponse] =
    ???
}

object StateOrphanService {
  def instance(context: NodeContext, events: Queue[IO, NodeState]): StateOrphanService =
    new StateOrphanService(context, events)
}

package trabica.fsm

import cats.effect.{IO, Ref}
import io.grpc.Metadata
import trabica.context.NodeContext
import trabica.model.{NodeError, NodeState}
import trabica.rpc.*

trait Node extends TrabicaFs2Grpc[IO, Metadata] {

  def interrupt: IO[Unit]

}

object Node {
  private object DeadNode extends Node {
    override val interrupt: IO[Unit] = IO.unit
    override def appendEntries(request: AppendEntriesRequest, ctx: Metadata): IO[AppendEntriesResponse] =
      IO.raiseError(NodeError.Uninitialized)
    override def vote(request: VoteRequest, ctx: Metadata): IO[VoteResponse] =
      IO.raiseError(NodeError.Uninitialized)
    override def join(request: JoinRequest, ctx: Metadata): IO[JoinResponse] =
      IO.raiseError(NodeError.Uninitialized)
  }

  def dead(context: NodeContext, state: Ref[IO, NodeState]): Node =
    DeadNode
}

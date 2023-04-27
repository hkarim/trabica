package trabica.fsm

import cats.effect.{IO, Ref}
import io.grpc.Metadata
import trabica.context.NodeContext
import trabica.model.{NodeError, NodeState}
import trabica.rpc.*

trait Node {

  def context: NodeContext

  def state: Ref[IO, NodeState]

  def server: TrabicaFs2Grpc[IO, Metadata]

  def interrupt: IO[Unit]

}

object Node {
  private class DeadNode(val context: NodeContext, val state: Ref[IO, NodeState]) extends Node {
    override val interrupt: IO[Unit] = IO.unit
    override val server: TrabicaFs2Grpc[IO, Metadata] = new TrabicaFs2Grpc[IO, Metadata] {
      override def appendEntries(request: AppendEntriesRequest, ctx: Metadata): IO[AppendEntriesResponse] =
        IO.raiseError(NodeError.Uninitialized)
      override def vote(request: VoteRequest, ctx: Metadata): IO[VoteResponse] =
        IO.raiseError(NodeError.Uninitialized)
      override def join(request: JoinRequest, ctx: Metadata): IO[JoinResponse] =
        IO.raiseError(NodeError.Uninitialized)
    }
  }

  def dead(context: NodeContext, state: Ref[IO, NodeState]): Node =
    new DeadNode(context, state)
}

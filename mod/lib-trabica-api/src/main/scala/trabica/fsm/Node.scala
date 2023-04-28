package trabica.fsm

import cats.effect.*
import cats.effect.std.Queue
import io.grpc.Metadata
import trabica.model.{Event, NodeError, NodeState}
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

  def dead: Node = DeadNode

  def termCheck(header: Header, currentState: NodeState, events: Queue[IO, Event]): IO[Unit] =
    for {
      peer <- header.peer.required
      _ <-
        if header.term > currentState.currentTerm then {
          val newState = NodeState.Follower(
            id = currentState.id,
            self = currentState.self,
            peers = Set(peer),
            leader = peer,
            currentTerm = header.term,
            votedFor = None,
            commitIndex = currentState.commitIndex,
            lastApplied = currentState.lastApplied,
          )
          events.offer(Event.NodeStateChanged(newState))
        } else IO.unit

    } yield ()
}

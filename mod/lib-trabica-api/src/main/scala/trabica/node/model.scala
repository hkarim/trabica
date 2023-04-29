package trabica.node

import cats.effect.*
import trabica.model.NodeError

extension [A](self: Option[A]) {
  def required: IO[A] = self match {
    case Some(value) =>
      IO.pure(value)
    case None =>
      IO.raiseError(NodeError.InvalidMessage)
  }
}

case class NodeTrace(
  orphanId: Int,
  followerId: Int,
  candidateId: Int,
  leaderId: Int,
)

object NodeTrace {
  def instance: NodeTrace =
    NodeTrace(0, 0, 0, 0)
}

extension (self: Ref[IO, NodeTrace]) {
  def incrementOrphan: IO[NodeTrace] =
    self.modify(t => (t, t.copy(orphanId = t.orphanId + 1)))

  def incrementFollower: IO[NodeTrace] =
    self.modify(t => (t, t.copy(followerId = t.followerId + 1)))

  def incrementCandidate: IO[NodeTrace] =
    self.modify(t => (t, t.copy(candidateId = t.candidateId + 1)))

  def incrementLeader: IO[NodeTrace] =
    self.modify(t => (t, t.copy(leaderId = t.leaderId + 1)))

}
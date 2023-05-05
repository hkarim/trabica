package trabica.node

import cats.effect.*
import com.google.protobuf.CodedInputStream
import trabica.model.{LogEntry, LogEntryTag, NodeError, Quorum}

type Interrupt = Deferred[IO, Either[Throwable, Unit]]

object Interrupt {
  def instance: IO[Interrupt] = Deferred[IO, Either[Throwable, Unit]]
}

extension (self: LogEntry) {
  def quorum: IO[Option[Quorum]] =
    if self.tag == LogEntryTag.Conf then {
      IO.delay {
        Some(
          Quorum.parseFrom(CodedInputStream.newInstance(self.data.asReadOnlyByteBuffer()))
        )
      }
    } else IO.pure(None)
}

extension [A](self: Option[A]) {
  def required(error: => NodeError): IO[A] = self match {
    case Some(value) =>
      IO.pure(value)
    case None =>
      IO.raiseError(error)
  }
}

case class NodeTrace(
  followerId: Int,
  candidateId: Int,
  leaderId: Int,
)

object NodeTrace {
  def instance: NodeTrace =
    NodeTrace(0, 0, 0)
}

extension (self: Ref[IO, NodeTrace]) {

  def incrementFollower: IO[NodeTrace] =
    self.updateAndGet(t => t.copy(followerId = t.followerId + 1))

  def incrementCandidate: IO[NodeTrace] =
    self.updateAndGet(t => t.copy(candidateId = t.candidateId + 1))

  def incrementLeader: IO[NodeTrace] =
    self.updateAndGet(t => t.copy(leaderId = t.leaderId + 1))

}

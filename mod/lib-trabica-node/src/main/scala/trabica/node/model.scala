package trabica.node

import cats.Show
import cats.effect.*
import com.google.protobuf.CodedInputStream
import trabica.model.*
import trabica.net.NodeApi

type Interrupt = Deferred[IO, Either[Throwable, Unit]]

object Interrupt {
  def instance: IO[Interrupt] = Deferred[IO, Either[Throwable, Unit]]
}

extension (self: LogEntry) {
  def cluster: IO[Option[Cluster]] =
    if self.tag == LogEntryTag.Conf then {
      IO.delay {
        Some(
          Cluster.parseFrom(CodedInputStream.newInstance(self.data.asReadOnlyByteBuffer()))
        )
      }
    } else IO.pure(None)
}

extension (self: Option[Header]) {
  def required: IO[Header] =
    IO.fromOption(self)(NodeError.MissingHeader)
}

extension [A](self: Option[A]) {
  def required(error: => NodeError): IO[A] =
    IO.fromOption(self)(error)
}

extension [S <: NodeState](self: S) {
  def updated(localState: LocalState)(using lens: NodeStateLens[S]): S =
    lens.updated(self, localState)
}

given Show[NodeApi] with {
  override def show(instance: NodeApi): String =
    s"${instance.memberPeer.host}:${instance.memberPeer.port}"
}

given Show[Peer] with {
  override def show(instance: Peer): String =
    s"${instance.host}:${instance.port}"
}

given Show[Vector[Peer]] with {
  override def show(instance: Vector[Peer]): String =
    instance
      .map(peer => s"${peer.host}:${peer.port}")
      .mkString("[", ", ", "]")
}

given Show[Map[Peer, Index]] with {
  override def show(instance: Map[Peer, Index]): String =
    instance.map { (peer, index) =>
      s"${peer.host}:${peer.port}->$index"
    }.mkString("[", ", ", "]")
}

given Show[Member] with {
  override def show(instance: Member): String =
    s"${instance.id}@${instance.peer.map(_.host).getOrElse("unknown")}:${instance.peer.map(_.port).getOrElse(0)}"
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

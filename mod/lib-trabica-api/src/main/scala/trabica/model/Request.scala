package trabica.model

import scodec.*
import scodec.bits.ByteVector

sealed trait Request {
  def header: Header
}

object Request {

  given Codec[Request] =
    codecs
      .discriminated[Request]
      .by(Codec[Tag])
      .caseP[AppendEntries](Tag.Request.AppendEntries) { case v: AppendEntries => v }(identity)(Codec[AppendEntries])
      .caseP[RequestVote](Tag.Request.RequestVote) { case v: RequestVote => v }(identity)(Codec[RequestVote])
      .caseP[Join](Tag.Request.Join) { case v: Join => v }(identity)(Codec[Join])

  final case class AppendEntries(
    header: Header,
    prevLogIndex: Index,
    prevLogTerm: Term,
    entries: Vector[ByteVector],
    leaderCommitIndex: Index,
  ) extends Request derives Codec

  final case class RequestVote(
    header: Header,
    lastLogIndex: Index,
    lastLogTerm: Term,
  ) extends Request derives Codec

  final case class Join(
    header: Header
  ) extends Request derives Codec

}

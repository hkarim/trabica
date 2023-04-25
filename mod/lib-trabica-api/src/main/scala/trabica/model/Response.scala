package trabica.model

import scodec.*

sealed trait Response {
  def header: Header
}

object Response {

  given Codec[Response] =
    codecs
      .discriminated[Response]
      .by(Codec[Tag])
      .caseP[AppendEntries](Tag.Request.AppendEntries) { case v: AppendEntries => v }(identity)(Codec[AppendEntries])
      .caseP[RequestVote](Tag.Request.RequestVote) { case v: RequestVote => v }(identity)(Codec[RequestVote])
      .caseP[Join](Tag.Request.Join) { case v: Join => v }(identity)(Codec[Join])

  final case class AppendEntries(
    header: Header,
    success: Boolean,
  ) extends Response derives Codec

  final case class RequestVote(
    header: Header,
    voteGranted: Boolean,
  ) extends Response derives Codec

  sealed trait JoinStatus
  object JoinStatus {
    given Codec[JoinStatus] =
      codecs
        .discriminated[JoinStatus]
        .by(codecs.uint8)
        .caseP[Accepted.type](0) { case Accepted => Accepted }(identity)(codecs.provide(Accepted))
        .caseP[UnknownLeader](1) { case v: UnknownLeader => v }(identity)(Codec[UnknownLeader])
        .caseP[Forward](2) { case v: Forward => v }(identity)(Codec[Forward])

    case object Accepted                               extends JoinStatus
    case class UnknownLeader(knownPeers: Vector[Peer]) extends JoinStatus derives Codec
    case class Forward(leader: Peer)                   extends JoinStatus derives Codec
  }

  final case class Join(
    header: Header,
    status: JoinStatus,
  ) extends Response derives Codec

}

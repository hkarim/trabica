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


  final case class AppendEntries(
    header: Header,
    success: Boolean,
  ) extends Response derives Codec


  final case class RequestVote(
    header: Header,
    voteGranted: Boolean,
  ) extends Response derives Codec

}

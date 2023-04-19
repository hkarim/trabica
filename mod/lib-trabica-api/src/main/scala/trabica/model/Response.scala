package trabica.model

import io.circe.*
import io.circe.syntax.*

sealed trait Response {
  def header: Header
  def widen: Response = this
}

object Response {

  given Encoder[Response] =
    Encoder.instance {
      case v: AppendEntries =>
        Encoder[AppendEntries].apply(v)
      case v: RequestVote =>
        Encoder[RequestVote].apply(v)
    }

  given Decoder[Response] =
    Decoder[Header].at("header").map(_.tag).flatMap {
      case Tag.Response.AppendEntries =>
        Decoder[AppendEntries].map(_.widen)
      case Tag.Response.RequestVote =>
        Decoder[RequestVote].map(_.widen)
      case otherwise =>
        Decoder.failed(DecodingFailure(s"unrecognized response tag `$otherwise`", Nil))
    }

  final case class AppendEntries(
    id: MessageId,
    term: Term,
    success: Boolean,
  ) extends Response {
    val header: Header = Header(
      id = id,
      version = Version.V1_0,
      tag = Tag.Response.AppendEntries,
      term = term,
    )
  }

  object AppendEntries {

    given Encoder[AppendEntries] =
      Encoder.instance { v =>
        Json.obj(
          "header"  -> v.header.asJson,
          "success" -> v.success.asJson,
        )
      }

    given Decoder[AppendEntries] =
      for {
        header  <- Decoder[Header].at("header")
        success <- Decoder[Boolean].at("success")
      } yield AppendEntries(
        id = header.id,
        term = header.term,
        success = success,
      )
  }

  final case class RequestVote(
    id: MessageId,
    term: Term,
    voteGranted: Boolean,
  ) extends Response {
    val header: Header = Header(
      id = id,
      version = Version.V1_0,
      tag = Tag.Response.RequestVote,
      term = term,
    )
  }

  object RequestVote {

    given Encoder[RequestVote] =
      Encoder.instance { v =>
        Json.obj(
          "header"      -> v.header.asJson,
          "voteGranted" -> v.voteGranted.asJson,
        )
      }

    given Decoder[RequestVote] =
      for {
        header      <- Decoder[Header].at("header")
        voteGranted <- Decoder[Boolean].at("voteGranted")
      } yield RequestVote(
        id = header.id,
        term = header.term,
        voteGranted = voteGranted,
      )
  }
}

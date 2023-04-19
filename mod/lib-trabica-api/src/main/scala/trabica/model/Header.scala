package trabica.model

import io.circe.*
import io.circe.syntax.*

final case class Header(
  id: MessageId,
  version: Version,
  tag: Tag,
  term: Term,
)

object Header {

  given Encoder[Header] =
    Encoder.instance[Header] { v =>
      Json.obj(
        "id"      -> v.id.asJson,
        "version" -> v.version.asJson,
        "tag"     -> v.tag.asJson,
        "term"    -> v.term.asJson,
      )
    }

  given Decoder[Header] =
    for {
      id      <- Decoder[MessageId].at("id")
      version <- Decoder[Version].at("version")
      tag     <- Decoder[Tag].at("tag")
      term    <- Decoder[Term].at("term")
    } yield Header(
      id = id,
      version = version,
      tag = tag,
      term = term,
    )
}

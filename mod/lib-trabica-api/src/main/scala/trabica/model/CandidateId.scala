package trabica.model

import io.circe.*
import io.circe.syntax.*

case class CandidateId(
  id: NodeId,
  ip: String,
  port: Int,
)

object CandidateId {
  given Encoder[CandidateId] =
    Encoder.instance { v =>
      Json.obj(
        "id"   -> v.id.asJson,
        "ip"   -> v.ip.asJson,
        "port" -> v.port.asJson,
      )
    }

  given Decoder[CandidateId] =
    for {
      id   <- Decoder[NodeId].at("id")
      ip   <- Decoder[String].at("ip")
      port <- Decoder[Int].at("port")
    } yield CandidateId(
      id = id,
      ip = ip,
      port = port,
    )
}

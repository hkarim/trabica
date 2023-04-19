package trabica.model

import io.circe.*
import io.circe.syntax.*

final case class LeaderId(
  ip: String,
  port: Int,
)

object LeaderId {
  given Encoder[LeaderId] =
    Encoder.instance { v =>
      Json.obj(
        "ip"   -> v.ip.asJson,
        "port" -> v.port.asJson,
      )
    }

  given Decoder[LeaderId] =
    for {
      ip   <- Decoder[String].at("ip")
      port <- Decoder[Int].at("port")
    } yield LeaderId(
      ip = ip,
      port = port,
    )
}

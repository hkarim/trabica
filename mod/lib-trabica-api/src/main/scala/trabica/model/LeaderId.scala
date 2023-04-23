package trabica.model

import fs2.protocols.*
import com.comcast.ip4s.*
import scodec.Codec

final case class LeaderId(
  id: NodeId,
  ip: Ipv4Address,
  port: Port,
)

object LeaderId {

  given Codec[LeaderId] =
    (Codec[NodeId] :: Ip4sCodecs.ipv4 :: Ip4sCodecs.port).as[LeaderId]

}

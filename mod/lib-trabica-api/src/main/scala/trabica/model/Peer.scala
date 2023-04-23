package trabica.model

import com.comcast.ip4s.*
import fs2.protocols.Ip4sCodecs
import scodec.Codec

case class Peer(
  ip: Ipv4Address,
  port: Port,
)

object Peer {
  given Codec[Peer] =
    (Ip4sCodecs.ipv4 :: Ip4sCodecs.port).as[Peer]
}

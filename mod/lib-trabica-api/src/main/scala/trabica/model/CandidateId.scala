package trabica.model

import fs2.protocols.*
import com.comcast.ip4s.*
import scodec.Codec

case class CandidateId(
  id: NodeId,
  ip: Ipv4Address,
  port: Port,
)

object CandidateId {
  given Codec[CandidateId] =
    (Codec[NodeId] :: Ip4sCodecs.ipv4 :: Ip4sCodecs.port).as[CandidateId]
}

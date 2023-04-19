package trabica.model

import io.circe.{Decoder, Encoder}

opaque type NodeId = String

object NodeId {
  given Encoder[NodeId] = Encoder.encodeString
  given Decoder[NodeId] = Decoder.decodeString
  def fromString(value: String): NodeId = value
}

 

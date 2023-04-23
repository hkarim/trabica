package trabica.model

import scodec.{Codec, codecs}

import java.util.UUID

opaque type NodeId = UUID

object NodeId {

  given Codec[NodeId] = codecs.uuid

  def fromUUID(value: UUID): NodeId = value
}

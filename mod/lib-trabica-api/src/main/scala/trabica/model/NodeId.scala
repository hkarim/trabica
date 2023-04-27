package trabica.model

import java.util.UUID

opaque type NodeId = UUID

object NodeId {

  def fromUUID(value: UUID): NodeId = value
}

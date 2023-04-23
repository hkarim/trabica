package trabica.model

import scodec.Codec
import scodec.codecs.*

opaque type MessageId = Long

object MessageId {
  given Codec[MessageId] = scodec.codecs.uint32

  final val zero: MessageId = 0L

  extension (self: MessageId) {
    def increment: MessageId = self + 1
  }
}

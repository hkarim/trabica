package trabica.model

import io.circe.{Decoder, Encoder}

opaque type MessageId = Long

object MessageId {
  given Encoder[MessageId] = Encoder.encodeLong
  given Decoder[MessageId] = Decoder.decodeLong
  
  final val zero: MessageId = 0L
  
  extension (self: MessageId) {
    def increment: MessageId = self + 1
  }
}

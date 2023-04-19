package trabica.model

import io.circe.*

opaque type Index = Long

object Index {
  given Encoder[Index] = Encoder.encodeLong
  given Decoder[Index] = Decoder.decodeLong

  final val zero : Index = 0
  final val one : Index = 1

  extension (self: Index) {
    def increment: Index = self + 1
  }
}

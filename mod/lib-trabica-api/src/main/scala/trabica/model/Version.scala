package trabica.model

import io.circe.{Decoder, Encoder}

opaque type Version = String

object Version {
  final val V1_0: Version = "1.0"

  given Encoder[Version] = Encoder.encodeString
  given Decoder[Version] = Decoder.decodeString
}



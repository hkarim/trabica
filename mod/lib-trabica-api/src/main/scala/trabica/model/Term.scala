package trabica.model

import io.circe.*

opaque type Term = Int

object Term {
  given Encoder[Term] = Encoder.encodeInt
  given Decoder[Term] = Decoder.decodeInt
  
  final val zero: Term = 0
}

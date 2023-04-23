package trabica.model

import scodec.Codec

opaque type Term = Long

object Term {
  given Codec[Term]    = scodec.codecs.uint32
  final val zero: Term = 0L
}

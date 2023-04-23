package trabica.model

import scodec.Codec

opaque type Index = Long

object Index {
  
  given Codec[Index] = scodec.codecs.uint32

  final val zero : Index = 0L
  final val one : Index = 1L

  extension (self: Index) {
    def increment: Index = self + 1
  }
}

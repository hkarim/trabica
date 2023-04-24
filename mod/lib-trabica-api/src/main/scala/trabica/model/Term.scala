package trabica.model

import scodec.Codec

import scala.annotation.*

opaque type Term = Long

object Term {
  given Codec[Term] = scodec.codecs.uint32

  final val zero: Term = 0L

  extension (self: Term) {
    @targetName("lt")
    def <(that: Term): Boolean = self < that
  }
}

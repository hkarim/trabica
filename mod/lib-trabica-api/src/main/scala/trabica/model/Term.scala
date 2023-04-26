package trabica.model

import scodec.Codec

import scala.annotation.*

opaque type Term = Long

object Term {
  given Codec[Term] = scodec.codecs.uint32

  final val zero: Term = 0L

  extension (self: Term) {
    def increment: Term = self + 1
    
    @targetName("lt")
    def <(that: Term): Boolean = self < that

    @targetName("le")
    def <=(that: Term): Boolean = self <= that

    @targetName("gt")
    def >(that: Term): Boolean = self > that

    @targetName("ge")
    def >=(that: Term): Boolean = self >= that
  }
}

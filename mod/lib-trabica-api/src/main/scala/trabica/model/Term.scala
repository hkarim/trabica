package trabica.model

import scala.annotation.targetName

opaque type Term = Long

object Term {
  
  final val zero: Term = 0L
  
  def of(value: Long): Term = value

  extension (self: Term) {

    def value: Long = self

    def increment: Term = self + 1

    @targetName("gt")
    def >(that: Term): Boolean = self > that
  }
}



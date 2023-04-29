package trabica.model

opaque type Index = Long

object Index {

  final val zero: Index = 0L

  final val one: Index = 1L

  extension (self: Index) {
    def increment: Index = self + 1
  }
}



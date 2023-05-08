package trabica.model

opaque type Index = Long

object Index {

  final val zero: Index = 0L

  final val one: Index = 1L

  def of(value: Long): Index = value

  extension (self: Index) {
    def value: Long      = self
    def increment: Index = self + 1
  }
}

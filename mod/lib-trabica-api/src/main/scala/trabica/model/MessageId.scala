package trabica.model

opaque type MessageId = Long

object MessageId {

  final val zero: MessageId = 0L

  extension (self: MessageId) {
    def value: Long = self
    def increment: MessageId = self + 1
  }
}

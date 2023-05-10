package trabica.model

sealed trait NodeError extends Throwable {
  override def getMessage: String
}

object NodeError {

  case object MissingHeader extends NodeError {
    override val getMessage: String = "missing message header"
  }

  case object InvalidHeader extends NodeError {
    override val getMessage: String = "invalid message header"
  }

  final case class ValueError(message: String) extends NodeError {
    override val getMessage: String = message
  }

  final case class StoreError(message: String) extends NodeError {
    override val getMessage: String = message
  }

}

package trabica.model

sealed trait NodeError extends Throwable {
  override def getMessage: String
}

object NodeError {

  case object InvalidMessage extends NodeError {
    override def getMessage: String = "invalid input message"
  }
  
  case class ValueError(message: String) extends NodeError {
    override def getMessage: String = message
  }
  
  case class StoreError(message: String) extends NodeError {
    override def getMessage: String = message
  }

}

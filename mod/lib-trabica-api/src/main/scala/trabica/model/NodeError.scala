package trabica.model

sealed trait NodeError extends Throwable {
  override def getMessage: String
}

object NodeError {
  case class InvalidConfig(key: String) extends NodeError {
    override def getMessage: String = s"invalid configuration for key: `$key`"
  }
  
  case class InvalidNodeState(state: NodeState) extends NodeError {
    override def getMessage: String = s"invalid node state $state"
  }
  
  case class SocketWriteError(e: Throwable) extends NodeError {
    override def getMessage: String = s"socket write error ${e.getMessage}"
  }

  case class SocketReadError(e: Throwable) extends NodeError {
    override def getMessage: String = s"socket read error ${e.getMessage}"
  }
}

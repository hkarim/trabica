package trabica.model

sealed trait Event

object Event {
  case class NodeStateEvent(nodeState: NodeState) extends Event
}


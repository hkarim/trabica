package trabica.model

sealed trait Event

object Event {
  case class NodeStateChangedEvent(nodeState: NodeState) extends Event
}

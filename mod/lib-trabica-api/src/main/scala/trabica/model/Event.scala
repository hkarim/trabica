package trabica.model

sealed trait Event

object Event {
  case class NodeStateChanged(newState: NodeState) extends Event
}

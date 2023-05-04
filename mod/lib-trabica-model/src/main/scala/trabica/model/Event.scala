package trabica.model

sealed trait Event

object Event {
  case class NodeStateChanged(
    oldState: NodeState,
    newState: NodeState,
    reason: StateTransitionReason
  ) extends Event
}

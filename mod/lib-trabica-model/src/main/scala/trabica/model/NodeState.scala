package trabica.model

enum NodeStateTag {
  case Follower
  case Candidate
  case Leader
}

sealed trait NodeState {
  def localState: LocalState
  def commitIndex: Index
  def lastApplied: Index
  def tag: NodeStateTag
  def leader: Option[Member]
}

object NodeState {

  final case class Follower(
    localState: LocalState,
    commitIndex: Index,
    lastApplied: Index,
    leader: Option[Member],
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Follower
  }

  final case class Candidate(
    localState: LocalState,
    commitIndex: Index,
    lastApplied: Index,
    votes: Set[Member],
    elected: Boolean,
    leader: Option[Member],
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Candidate
  }

  final case class Leader(
    localState: LocalState,
    commitIndex: Index,
    lastApplied: Index,
    nextIndex: Map[Peer, Index],
    matchIndex: Map[Peer, Index],
  ) extends NodeState {
    override val tag: NodeStateTag      = NodeStateTag.Leader
    override val leader: Option[Member] = localState.node
  }

}

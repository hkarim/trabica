package trabica.model

enum NodeStateTag {
  case Orphan
  case NonVoter
  case Follower
  case Candidate
  case Leader
  case Joint
}

sealed trait NodeState {
  def self: Peer
  def peers: Set[Peer]
  def currentTerm: Term
  def votedFor: Option[Peer]
  def commitIndex: Index
  def lastApplied: Index
  def tag: NodeStateTag
}

object NodeState {

  final case class Orphan(
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Orphan
  }

  final case class NonVoter(
    self: Peer,
    peers: Set[Peer],
    leader: Option[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.NonVoter
  }

  final case class Follower(
    self: Peer,
    peers: Set[Peer],
    leader: Peer,
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Follower
  }

  final case class Candidate(
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
    votes: Set[Peer],
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Candidate
  }

  final case class Leader(
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
    nextIndex: Map[Peer, Index],
    matchIndex: Map[Peer, Index],
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Leader
  }

  final case class Joint(
    self: Peer,
    peers: Set[Peer],
    leader: Option[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    override val tag: NodeStateTag = NodeStateTag.Joint
  }
}

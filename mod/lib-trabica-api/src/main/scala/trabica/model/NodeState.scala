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
  def id: NodeId
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
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.Orphan
  }

  final case class NonVoter(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    leader: Option[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.NonVoter
  }

  final case class Follower(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    leader: Peer,
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.Follower
  }

  final case class Candidate(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.Candidate
  }

  final case class Leader(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
    nextIndex: Index,
    matchIndex: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.Leader
  }

  final case class Joint(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    leader: Option[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState {
    val tag: NodeStateTag = NodeStateTag.Joint
  }
}

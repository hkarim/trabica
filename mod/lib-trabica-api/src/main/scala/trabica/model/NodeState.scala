package trabica.model

sealed trait NodeState {
  def id: NodeId
  def self: Peer
  def peers: Set[Peer]
  def currentTerm: Term
  def votedFor: Option[Peer]
  def commitIndex: Index
  def lastApplied: Index
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
  ) extends NodeState

  final case class NonVoter(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  final case class Follower(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  final case class Candidate(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

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
  ) extends NodeState

  final case class Joint(
    id: NodeId,
    self: Peer,
    peers: Set[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState
}

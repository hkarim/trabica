package trabica.model

sealed trait NodeState {
  def id: NodeId
  def currentTerm: Term
  def votedFor: Option[CandidateId]
  def commitIndex: Index
  def lastApplied: Index
}

object NodeState {
  final case class Leader(
    id: NodeId,
    currentTerm: Term,
    votedFor: Option[CandidateId],
    commitIndex: Index,
    lastApplied: Index,
    nextIndex: Index,
    matchIndex: Index,
  ) extends NodeState

  final case class Candidate(
    id: NodeId,
    currentTerm: Term,
    votedFor: Option[CandidateId],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  final case class Follower(
    id: NodeId,
    currentTerm: Term,
    votedFor: Option[CandidateId],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState
  
  object Follower {
    def fresh(id: NodeId): Follower = Follower(
      id = id,
      currentTerm = Term.zero,
      votedFor = None,
      commitIndex = Index.zero,
      lastApplied = Index.zero,
    )
  }

  final case class Joint(
    id: NodeId,
    currentTerm: Term,
    votedFor: Option[CandidateId],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState
}

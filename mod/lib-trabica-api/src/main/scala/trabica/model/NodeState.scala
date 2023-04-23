package trabica.model

import com.comcast.ip4s.*

sealed trait NodeState {
  def id: NodeId
  def self: Peer
  def peers: Vector[Peer]
  def currentTerm: Term
  def votedFor: Option[Peer]
  def commitIndex: Index
  def lastApplied: Index
}

object NodeState {

  final case class Follower(
    id: NodeId,
    self: Peer,
    peers: Vector[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  object Follower {
    def fresh(id: NodeId, ip: Ipv4Address, port: Port, peers: Vector[Peer]): Follower = Follower(
      id = id,
      self = Peer(
        ip = ip,
        port = port,
      ),
      peers = peers,
      currentTerm = Term.zero,
      votedFor = None,
      commitIndex = Index.zero,
      lastApplied = Index.zero,
    )
  }

  final case class Candidate(
    id: NodeId,
    self: Peer,
    peers: Vector[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  final case class NonVoter(
    id: NodeId,
    self: Peer,
    peers: Vector[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState

  final case class Leader(
    id: NodeId,
    self: Peer,
    peers: Vector[Peer],
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
    peers: Vector[Peer],
    currentTerm: Term,
    votedFor: Option[Peer],
    commitIndex: Index,
    lastApplied: Index,
  ) extends NodeState
}

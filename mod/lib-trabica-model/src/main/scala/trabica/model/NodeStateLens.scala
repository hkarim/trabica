package trabica.model

trait NodeStateLens[S <: NodeState] {
  def updated(state: S, localState: LocalState): S
  def updated(state: S, leader: QuorumNode): S
}

object NodeStateLens {
  def apply[S <: NodeState](using ev: NodeStateLens[S]): NodeStateLens[S] = ev

  given NodeStateLens[NodeState.Follower] with {
    def updated(state: NodeState.Follower, localState: LocalState): NodeState.Follower =
      state.copy(localState = localState)
    def updated(state: NodeState.Follower, leader: QuorumNode): NodeState.Follower =
      state.copy(leader = Some(leader))
  }

  given NodeStateLens[NodeState.Candidate] with {
    def updated(state: NodeState.Candidate, localState: LocalState): NodeState.Candidate =
      state.copy(localState = localState)
    def updated(state: NodeState.Candidate, leader: QuorumNode): NodeState.Candidate =
      state.copy(leader = Some(leader))
  }

  given NodeStateLens[NodeState.Leader] with {
    def updated(state: NodeState.Leader, localState: LocalState): NodeState.Leader =
      state.copy(localState = localState)
    def updated(state: NodeState.Leader, leader: QuorumNode): NodeState.Leader =
      state
  }

}

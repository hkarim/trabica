package trabica.model

trait NodeStateLens[S <: NodeState] {
  def updated(state: S, localState: LocalState): S
}

object NodeStateLens {
  def apply[S <: NodeState](using ev: NodeStateLens[S]): NodeStateLens[S] = ev

  given NodeStateLens[NodeState.NonVoter] with {
    def updated(state: NodeState.NonVoter, localState: LocalState): NodeState.NonVoter =
      state.copy(localState = localState)
  }

  given NodeStateLens[NodeState.Follower] with {
    def updated(state: NodeState.Follower, localState: LocalState): NodeState.Follower =
      state.copy(localState = localState)
  }

  given NodeStateLens[NodeState.Candidate] with {
    def updated(state: NodeState.Candidate, localState: LocalState): NodeState.Candidate =
      state.copy(localState = localState)
  }

  given NodeStateLens[NodeState.Leader] with {
    def updated(state: NodeState.Leader, localState: LocalState): NodeState.Leader =
      state.copy(localState = localState)
  }

}

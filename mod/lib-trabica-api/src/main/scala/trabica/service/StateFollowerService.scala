package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.{Header, NodeState, Request, Response}

class StateFollowerService(context: NodeContext) {

  def onRequest(state: NodeState.Follower, request: Request): IO[Response] =
    request match {
      case v: Request.AppendEntries =>
        onAppendEntries(state, v)
      case v: Request.RequestVote =>
        onRequestVote(state, v)
      case _: Request.Join =>
        onJoin(state)
    }

  private def onAppendEntries(state: NodeState.Follower, request: Request.AppendEntries): IO[Response.AppendEntries] =
    ???

  private def onRequestVote(state: NodeState.Follower, request: Request.RequestVote): IO[Response.RequestVote] =
    ???

  private def onJoin(state: NodeState.Follower): IO[Response.Join] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        status = Response.JoinStatus.Forward(leader = state.leader),
      )
    } yield response

}

object StateFollowerService {
  def instance(context: NodeContext): StateFollowerService =
    new StateFollowerService(context)
}

package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.*

class StateLeaderService(context: NodeContext) {

  def onRequest(state: NodeState.Leader, request: Request): IO[Response] =
    request match {
      case v: Request.AppendEntries =>
        onAppendEntries(state, v)
      case v: Request.RequestVote =>
        onRequestVote(state, v)
      case v: Request.Join =>
        onJoin(state, v)
    }

  private def onAppendEntries(state: NodeState.Leader, request: Request.AppendEntries): IO[Response.AppendEntries] =
    ???

  private def onRequestVote(state: NodeState.Leader, request: Request.RequestVote): IO[Response.RequestVote] =
    ???

  private def onJoin(state: NodeState.Leader, request: Request.Join): IO[Response.Join] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        status = Response.JoinStatus.Accepted,
      )
      // TODO append new cluster config entry to the leader log
      _ <- context.events.offer(
        Event.NodeStateChangedEvent(
          state.copy(peers = state.peers + request.header.peer)
        )
      )
    } yield response

}

object StateLeaderService {
  def instance(context: NodeContext): StateLeaderService =
    new StateLeaderService(context)
}

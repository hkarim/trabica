package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.{Header, NodeState, Request, Response}

class StateOrphanService(context: NodeContext) {

  def onRequest(state: NodeState.Orphan, request: Request): IO[Response] =
    request match {
      case v: Request.AppendEntries =>
        onAppendEntries(state, v)
      case v: Request.RequestVote =>
        onRequestVote(state, v)
      case _: Request.Join =>
        onJoin(state)
    }

  private def onAppendEntries(state: NodeState.Orphan, request: Request.AppendEntries): IO[Response.AppendEntries] =
    ???

  private def onRequestVote(state: NodeState.Orphan, request: Request.RequestVote): IO[Response.RequestVote] =
    ???

  private def onJoin(state: NodeState.Orphan): IO[Response.Join] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        status = Response.JoinStatus.UnknownLeader(knownPeers = state.peers.toVector),
      )
    } yield response
}

object StateOrphanService {
  def instance(context: NodeContext): StateOrphanService =
    new StateOrphanService(context)
}

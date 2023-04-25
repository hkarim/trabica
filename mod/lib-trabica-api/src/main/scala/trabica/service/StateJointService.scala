package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.{Header, NodeState, Request, Response}

class StateJointService(context: NodeContext) {

  def onRequest(state: NodeState.Joint, request: Request): IO[Response] =
    request match {
      case v: Request.AppendEntries =>
        onAppendEntries(state, v)
      case v: Request.RequestVote =>
        onRequestVote(state, v)
      case _: Request.Join =>
        onJoin(state)
    }

  private def onAppendEntries(state: NodeState.Joint, request: Request.AppendEntries): IO[Response.AppendEntries] =
    ???

  private def onRequestVote(state: NodeState.Joint, request: Request.RequestVote): IO[Response.RequestVote] =
    ???

  private def onJoin(state: NodeState.Joint): IO[Response.Join] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      status = state.leader match {
        case Some(value) =>
          Response.JoinStatus.Forward(leader = value)
        case None =>
          Response.JoinStatus.UnknownLeader(knownPeers = state.peers.toVector)
      }
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        status = status,
      )
    } yield response

}

object StateJointService {
  def instance(context: NodeContext): StateJointService =
    new StateJointService(context)
}
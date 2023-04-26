package trabica.service

import cats.effect.{Deferred, IO}
import trabica.context.NodeContext
import trabica.model.*

class StateLeaderService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[ServiceResponse] =
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case state: NodeState.Leader =>
          request match {
            case v: Request.AppendEntries =>
              onAppendEntries(state, v)
            case v: Request.RequestVote =>
              onRequestVote(state, v)
            case v: Request.Join =>
              onJoin(state, v)
          }
        case state =>
          IO.raiseError(NodeError.InvalidNodeState(state))
      }
    }
  private def onAppendEntries(state: NodeState.Leader, request: Request.AppendEntries): IO[ServiceResponse] =
    ???

  private def onRequestVote(state: NodeState.Leader, request: Request.RequestVote): IO[ServiceResponse] =
    ???

  private def onJoin(state: NodeState.Leader, request: Request.Join): IO[ServiceResponse] =
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
      signal <- Deferred[IO, Unit]
      newState = state.copy(peers = state.peers + request.header.peer, signal = signal)
    } yield ServiceResponse.Reload(newState, response)

}

object StateLeaderService {
  def instance(context: NodeContext): StateLeaderService =
    new StateLeaderService(context)
}

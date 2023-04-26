package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.*

class StateNonVoterService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[ServiceResponse] =
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case state: NodeState.NonVoter =>
          request match {
            case v: Request.AppendEntries =>
              onAppendEntries(state, v)
            case v: Request.RequestVote =>
              onRequestVote(state, v)
            case _: Request.Join =>
              onJoin(state)
          }
        case state =>
          IO.raiseError(NodeError.InvalidNodeState(state))
      }
    }
  private def onAppendEntries(state: NodeState.NonVoter, request: Request.AppendEntries): IO[ServiceResponse] =
    ???

  private def onRequestVote(state: NodeState.NonVoter, request: Request.RequestVote): IO[ServiceResponse] =
    ???

  private def onJoin(state: NodeState.NonVoter): IO[ServiceResponse] =
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
    } yield ServiceResponse.Pure(response)
}

object StateNonVoterService {
  def instance(context: NodeContext): StateNonVoterService =
    new StateNonVoterService(context)
}

package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.{Header, NodeError, NodeState, Request, Response}

class StateFollowerService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[Response] =
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case state: NodeState.Follower =>
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

  private def onAppendEntries(state: NodeState.Follower, request: Request.AppendEntries): IO[Response.AppendEntries] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.AppendEntries(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        success = state.currentTerm >= request.header.term,
      )
      _ <- state.heartbeat.offer(())
    } yield response

  private def onRequestVote(state: NodeState.Follower, request: Request.RequestVote): IO[Response.RequestVote] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.RequestVote(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        voteGranted = request.header.term >= state.currentTerm,
      )
    } yield response

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

package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.*

class StateFollowerService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[ServiceResponse] =
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

  private def onAppendEntries(state: NodeState.Follower, request: Request.AppendEntries): IO[ServiceResponse] =
    for {
      _ <- logger.debug(s"follower received AppendEntries request")
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.AppendEntries(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        success = state.currentTerm >= request.header.term,
      )
      _ <- logger.debug(s"updating heartbeat queue")
      _ <- context.heartbeat.offer(())
    } yield ServiceResponse.Pure(response)

  private def onRequestVote(state: NodeState.Follower, request: Request.RequestVote): IO[ServiceResponse] =
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
    } yield ServiceResponse.Pure(response)

  private def onJoin(state: NodeState.Follower): IO[ServiceResponse] =
    for {
      _ <- logger.debug(s"follower received Join request")
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        ),
        status = Response.JoinStatus.Forward(leader = state.leader),
      )
    } yield ServiceResponse.Pure(response)

}

object StateFollowerService {
  def instance(context: NodeContext): StateFollowerService =
    new StateFollowerService(context)
}

package trabica.service

import cats.effect.IO
import trabica.context.NodeContext
import trabica.model.{Header, NodeError, NodeState, Request, Response}

class StateCandidateService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[Response] =
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case state: NodeState.Candidate =>
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

  private def onAppendEntries(state: NodeState.Candidate, request: Request.AppendEntries): IO[Response.AppendEntries] =
    ???

  private def onRequestVote(state: NodeState.Candidate, request: Request.RequestVote): IO[Response.RequestVote] =
    ???

  private def onJoin(state: NodeState.Candidate): IO[Response.Join] =
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

object StateCandidateService {
  def instance(context: NodeContext): StateCandidateService =
    new StateCandidateService(context)
}

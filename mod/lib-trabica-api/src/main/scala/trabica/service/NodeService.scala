package trabica.service

import cats.effect.*
import trabica.context.NodeContext
import trabica.model.*

class NodeService(nodeContext: NodeContext) {

  def onRequest(request: Request): IO[Response] =
    request match {
      case rq: Request.AppendEntries =>
        onAppendEntries(rq)
      case rq: Request.RequestVote =>
        println(s"[NodeService::onRequest] $rq")
        onRequestVote(rq)
    }

  private def onAppendEntries(request: Request.AppendEntries): IO[Response.AppendEntries] =
    for {
      id    <- nodeContext.messageId.getAndUpdate(_.increment)
      state <- nodeContext.nodeState.get
    } yield Response.AppendEntries(
      header = Header(
        peer = state.self,
        messageId = id,
        term = state.currentTerm,
      ),
      success = request.header.term == state.currentTerm,
    )

  private def onRequestVote(request: Request.RequestVote): IO[Response.RequestVote] =
    for {
      id    <- nodeContext.messageId.getAndUpdate(_.increment)
      state <- nodeContext.nodeState.get
    } yield Response.RequestVote(
      header = Header(
        peer = state.self,
        messageId = id,
        term = state.currentTerm
      ),
      voteGranted = request.header.term == state.currentTerm
    )

}

object NodeService {
  def instance(nodeContext: NodeContext): NodeService =
    new NodeService(nodeContext)
}

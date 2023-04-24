package trabica.service

import cats.effect.*
import trabica.context.NodeContext
import trabica.model.*

class NodeService(context: NodeContext) {

  def onRequest(request: Request): IO[Response] =
    IO.println(s"[NodeService::onRequest] $request") >> {
      request match {
        case rq: Request.AppendEntries =>
          onAppendEntries(rq)
        case rq: Request.RequestVote =>
          onRequestVote(rq)
        case rq: Request.Join =>
          onJoin(rq)
      }
    }

  private def onAppendEntries(request: Request.AppendEntries): IO[Response.AppendEntries] =
    for {
      id    <- context.messageId.getAndUpdate(_.increment)
      state <- context.nodeState.get
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
      id    <- context.messageId.getAndUpdate(_.increment)
      state <- context.nodeState.get
    } yield Response.RequestVote(
      header = Header(
        peer = state.self,
        messageId = id,
        term = state.currentTerm
      ),
      voteGranted = request.header.term == state.currentTerm
    )

  private def onJoin(request: Request.Join): IO[Response.Join] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      state     <- context.nodeState.get
      response = Response.Join(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = state.currentTerm,
        )
      )
      _ <- state match {
        case v: NodeState.Leader =>
          context.events.offer(
            Event.NodeStateEvent(
              v.copy(peers = v.peers + request.header.peer)
            )
          )
        case _ =>
          IO.unit
      }
    } yield response

}

object NodeService {
  def instance(nodeContext: NodeContext): NodeService =
    new NodeService(nodeContext)
}

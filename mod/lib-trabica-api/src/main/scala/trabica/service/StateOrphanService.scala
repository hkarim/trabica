package trabica.service

import cats.effect.{Deferred, IO}
import trabica.context.NodeContext
import trabica.model.*

class StateOrphanService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[ServiceResponse] =
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case state: NodeState.Orphan =>
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

  private def onAppendEntries(state: NodeState.Orphan, request: Request.AppendEntries): IO[ServiceResponse] = {
    def newState(signal: Deferred[IO, Unit]) = NodeState.Follower(
      id = state.id,
      self = state.self,
      peers = state.peers + request.header.peer,
      leader = request.header.peer,
      currentTerm = request.header.term,
      votedFor = None,
      commitIndex = Index.zero,
      lastApplied = Index.zero,
      signal = signal,
    )

    for {
      _         <- logger.info(s"orphan received AppendEntries, will trigger state change to follower")
      messageId <- context.messageId.getAndUpdate(_.increment)
      response = Response.AppendEntries(
        header = Header(
          peer = state.self,
          messageId = messageId,
          term = request.header.term,
        ),
        success = true,
      )
      signal <- Deferred[IO, Unit]
    } yield ServiceResponse.Reload(newState(signal), response)
  }

  private def onRequestVote(state: NodeState.Orphan, request: Request.RequestVote): IO[ServiceResponse] =
    ???

  private def onJoin(state: NodeState.Orphan): IO[ServiceResponse] =
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
    } yield ServiceResponse.Pure(response)
}

object StateOrphanService {
  def instance(context: NodeContext): StateOrphanService =
    new StateOrphanService(context)
}

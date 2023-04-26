package trabica.service

import cats.effect.IO
import cats.effect.kernel.Deferred
import trabica.context.NodeContext
import trabica.model.*

class StateCandidateService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  def onRequest(request: Request): IO[ServiceResponse] =
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

  private def onAppendEntries(state: NodeState.Candidate, request: Request.AppendEntries): IO[ServiceResponse] =
    for {
      messageId <- context.messageId.getAndUpdate(_.increment)
      serviceResponse <-
        if request.header.term > state.currentTerm then {
          def newState(signal: Deferred[IO, Unit]) =
            NodeState.Follower(
              id = state.id,
              self = state.self,
              peers = state.peers,
              leader = request.header.peer,
              currentTerm = request.header.term,
              votedFor = None,
              commitIndex = state.commitIndex,
              lastApplied = state.lastApplied,
              signal = signal,
            )
          val response =
            Response.AppendEntries(
              header = Header(
                peer = state.self,
                messageId = messageId,
                term = request.header.term,
              ),
              success = true,
            )
          Deferred[IO, Unit].map { signal =>
            ServiceResponse.Reload(newState(signal), response)
          }

        } else {
          val response =
            Response.AppendEntries(
              header = Header(
                peer = state.self,
                messageId = messageId,
                term = state.currentTerm,
              ),
              success = false,
            )
          IO.pure(ServiceResponse.Pure(response))

        }

    } yield serviceResponse

  private def onRequestVote(state: NodeState.Candidate, request: Request.RequestVote): IO[ServiceResponse] =
    ???

  private def onJoin(state: NodeState.Candidate): IO[ServiceResponse] =
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

object StateCandidateService {
  def instance(context: NodeContext): StateCandidateService =
    new StateCandidateService(context)
}

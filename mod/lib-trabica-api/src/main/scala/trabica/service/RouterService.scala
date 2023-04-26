package trabica.service

import cats.effect.*
import trabica.context.NodeContext
import trabica.model.*

class RouterService(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  private val stateOrphanService: StateOrphanService =
    StateOrphanService.instance(context)

  private val stateNonVoterService: StateNonVoterService =
    StateNonVoterService.instance(context)

  private val stateFollowerService: StateFollowerService =
    StateFollowerService.instance(context)

  private val stateCandidateService: StateCandidateService =
    StateCandidateService.instance(context)

  private val stateLeaderService: StateLeaderService =
    StateLeaderService.instance(context)

  private val stateJointService: StateJointService =
    StateJointService.instance(context)

  def onRequest(request: Request): IO[Response] =
    logger.debug(s"routing request: $request") >>
      context.nodeState.get.flatMap {
        case _: NodeState.Orphan =>
          stateOrphanService.onRequest(request)
        case _: NodeState.NonVoter =>
          stateNonVoterService.onRequest(request)
        case _: NodeState.Follower =>
          stateFollowerService.onRequest(request)
        case _: NodeState.Candidate =>
          stateCandidateService.onRequest(request)
        case _: NodeState.Leader =>
          stateLeaderService.onRequest(request)
        case _: NodeState.Joint =>
          stateJointService.onRequest(request)
      }.flatMap(handle)

  private def handle(serviceResponse: ServiceResponse): IO[Response] =
    serviceResponse match {
      case ServiceResponse.Pure(response) =>
        IO.pure(response)
      case ServiceResponse.Patch(newState, response) =>
        context.nodeState.set(newState).as(response)
      case ServiceResponse.Reload(newState, response) =>
        OperationService.stateChanged(context, newState).as(response)
    }

}

object RouterService {
  def instance(nodeContext: NodeContext): RouterService =
    new RouterService(nodeContext)
}

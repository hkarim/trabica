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
    logger.debug(s"$request") >> {
      context.nodeState.get.flatMap {
        case v: NodeState.Orphan =>
          stateOrphanService.onRequest(v, request)
        case v: NodeState.NonVoter =>
          stateNonVoterService.onRequest(v, request)
        case v: NodeState.Follower =>
          stateFollowerService.onRequest(v, request)
        case v: NodeState.Candidate =>
          stateCandidateService.onRequest(v, request)
        case v: NodeState.Leader =>
          stateLeaderService.onRequest(v, request)
        case v: NodeState.Joint =>
          stateJointService.onRequest(v, request)
      }
    }

}

object RouterService {
  def instance(nodeContext: NodeContext): RouterService =
    new RouterService(nodeContext)
}

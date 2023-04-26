package trabica.model

sealed trait ServiceResponse

object ServiceResponse {
  case class Pure(response: Response) extends ServiceResponse
  case class Patch(state: NodeState, response: Response) extends ServiceResponse
  case class Reload(state: NodeState, response: Response) extends ServiceResponse
}




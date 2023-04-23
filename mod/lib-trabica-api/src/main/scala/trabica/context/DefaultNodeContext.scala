package trabica.context

import cats.effect.std.Supervisor
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import trabica.model.{CliCommand, MessageId, NodeState}
import trabica.net.Server
import trabica.service.{InitService, VoteStream}

class DefaultNodeContext(
  val config: Config,
  val nodeState: Ref[IO, NodeState],
  val messageId: Ref[IO, MessageId],
) extends NodeContext

object DefaultNodeContext {
  def run(command: CliCommand): IO[Unit] = {
    val config: Config           = ConfigFactory.load()
    val initService: InitService = InitService.instance(command)
    Supervisor[IO].use { supervisor =>
      for {
        nodeState <- initService.state
        messageId <- Ref.of[IO, MessageId](MessageId.zero)
        nodeContext = new DefaultNodeContext(
          config = config,
          nodeState = nodeState,
          messageId = messageId
        )
        _      <- supervisor.supervise(VoteStream.instance(nodeContext).run)
        server <- Server.instance(nodeContext).run
      } yield server
    }

  }
}

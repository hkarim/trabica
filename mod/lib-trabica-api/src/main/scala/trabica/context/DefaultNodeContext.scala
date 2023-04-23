package trabica.context

import cats.effect.std.{Supervisor, UUIDGen}
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import trabica.model.{MessageId, NodeId, NodeState}
import trabica.net.Server
import trabica.service.VoteStream

class DefaultNodeContext(
  val config: Config,
  val nodeState: Ref[IO, NodeState],
  val messageId: Ref[IO, MessageId],
) extends NodeContext

object DefaultNodeContext {
  def run: IO[Unit] = {
    val config: Config = ConfigFactory.load()
    Supervisor[IO].use { supervisor =>
      for {
        uuid <- UUIDGen.randomUUID[IO]
        nodeId = NodeId.fromUUID(uuid)
        nodeState <- Ref.of[IO, NodeState](NodeState.Follower.fresh(nodeId))
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

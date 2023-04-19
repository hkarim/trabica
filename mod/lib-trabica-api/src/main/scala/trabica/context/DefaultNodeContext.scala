package trabica.context

import cats.effect.std.UUIDGen
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import trabica.model.{MessageId, NodeId, NodeState}
import trabica.net.Server

class DefaultNodeContext(
  val config: Config,
  val nodeState: Ref[IO, NodeState],
  val messageId: Ref[IO, MessageId],
) extends NodeContext

object DefaultNodeContext {
  def run: IO[Unit] = {
    val config: Config = ConfigFactory.load()
    for {
      uuid <- UUIDGen.randomString[IO]
      nodeId = NodeId.fromString(uuid)
      nodeState <- Ref.of[IO, NodeState](NodeState.Follower.fresh(nodeId))
      messageId <- Ref.of[IO, MessageId](MessageId.zero)
      nodeContext = new DefaultNodeContext(
        config = config,
        nodeState = nodeState,
        messageId = messageId
      )
      server <- Server.instance(nodeContext).run
    } yield server
  }
}

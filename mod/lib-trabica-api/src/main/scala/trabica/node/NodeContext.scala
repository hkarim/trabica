package trabica.node

import cats.effect.{IO, Ref}
import com.typesafe.config.Config
import trabica.model.MessageId

case class NodeContext(
  config: Config,
  messageId: Ref[IO, MessageId],
)

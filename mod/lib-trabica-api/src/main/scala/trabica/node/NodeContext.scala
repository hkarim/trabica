package trabica.node

import cats.effect.{IO, Ref}
import com.typesafe.config.Config
import trabica.model.{MessageId, Peer}

case class NodeContext(
  config: Config,
  peer: Peer,
  messageId: Ref[IO, MessageId],
)

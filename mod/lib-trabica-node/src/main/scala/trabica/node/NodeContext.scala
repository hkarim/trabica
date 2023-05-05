package trabica.node

import cats.effect.{IO, Ref}
import com.typesafe.config.Config
import trabica.model.MessageId
import trabica.net.Networking
import trabica.store.FsmStore

case class NodeContext(
  config: Config,
  messageId: Ref[IO, MessageId],
  networking: Networking,
  store: FsmStore,
)

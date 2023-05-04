package trabica.node

import cats.effect.{IO, Ref}
import com.typesafe.config.Config
import trabica.model.{MessageId, Peer}
import trabica.net.Networking
import trabica.store.FsmStore

case class NodeContext(
  config: Config,
  peer: Peer,
  messageId: Ref[IO, MessageId],
  networking: Networking,
  store: FsmStore,
)

package trabica.node

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{IO, Ref}
import com.typesafe.config.Config
import trabica.model.{Event, MessageId, Peer}
import trabica.net.Networking
import trabica.store.FsmStore

case class NodeContext(
  config: Config,
  messageId: Ref[IO, MessageId],
  networking: Networking,
  events: Queue[IO, Event],
  supervisor: Supervisor[IO],
  store: FsmStore,
  quorumId: String,
  quorumPeer: Peer,
)

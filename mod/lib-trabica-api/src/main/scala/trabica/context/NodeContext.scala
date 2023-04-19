package trabica.context

import cats.effect.*
import com.typesafe.config.Config
import trabica.model.{MessageId, NodeState}

trait NodeContext {
  def config: Config
  def nodeState: Ref[IO, NodeState]
  def messageId: Ref[IO, MessageId]
}

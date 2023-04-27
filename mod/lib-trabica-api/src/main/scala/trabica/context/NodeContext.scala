package trabica.context

import cats.effect.*
import com.typesafe.config.Config
import trabica.model.*

trait NodeContext {
  def config: Config
  def messageId: Ref[IO, MessageId]
}

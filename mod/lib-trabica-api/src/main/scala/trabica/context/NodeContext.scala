package trabica.context

import cats.effect.*
import cats.effect.std.Queue
import com.typesafe.config.Config
import fs2.concurrent.SignallingRef
import trabica.model.*

trait NodeContext {
  def config: Config
  def nodeState: Ref[IO, NodeState]
  def messageId: Ref[IO, MessageId]
  def events: Queue[IO, Event]
  def interrupt: SignallingRef[IO, Boolean]
}

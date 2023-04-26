package trabica.context

import cats.effect.*
import cats.effect.std.Queue
import com.typesafe.config.Config
import trabica.model.*

trait NodeContext {
  def config: Config
  def nodeState: Ref[IO, NodeState]
  def messageId: Ref[IO, MessageId]
  def heartbeat: Queue[IO, Unit]
  def orphanId : Ref[IO, Int]
  def followerId : Ref[IO, Int]
  def leaderId : Ref[IO, Int]
}

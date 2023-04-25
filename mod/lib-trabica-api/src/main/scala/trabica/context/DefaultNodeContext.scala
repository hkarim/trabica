package trabica.context

import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.concurrent.SignallingRef
import trabica.model.*
import trabica.service.{OperationService, StateService}

class DefaultNodeContext(
  val config: Config,
  val nodeState: Ref[IO, NodeState],
  val messageId: Ref[IO, MessageId],
  val events: Queue[IO, Event],
  val interrupt: SignallingRef[IO, Boolean],
) extends NodeContext

object DefaultNodeContext {
  private def logging: IO[Unit] = IO.delay {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(scribe.Level.Debug))
      .replace()
  }.void

  def run(command: CliCommand): IO[Unit] =
    for {
      _         <- logging
      config    <- IO.blocking(ConfigFactory.load())
      nodeState <- StateService.state(command)
      messageId <- Ref.of[IO, MessageId](MessageId.zero)
      events    <- Queue.unbounded[IO, Event]
      interrupt <- SignallingRef.of[IO, Boolean](initial = false)
      nodeContext = new DefaultNodeContext(
        config = config,
        nodeState = nodeState,
        messageId = messageId,
        events = events,
        interrupt = interrupt,
      )
      _ <- OperationService.run(nodeContext)
    } yield ()

}

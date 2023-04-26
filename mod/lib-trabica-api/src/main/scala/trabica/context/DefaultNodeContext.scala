package trabica.context

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import trabica.model.*
import trabica.net.Server
import trabica.service.{OperationService, StateService}

class DefaultNodeContext(
  val config: Config,
  val nodeState: Ref[IO, NodeState],
  val messageId: Ref[IO, MessageId],
  val heartbeat: Queue[IO, Unit],
  val orphanId: Ref[IO, Int],
  val followerId: Ref[IO, Int],
  val leaderId: Ref[IO, Int],
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
    Supervisor[IO].use { supervisor =>
      for {
        _          <- logging
        config     <- IO.blocking(ConfigFactory.load())
        nodeState  <- StateService.state(command)
        messageId  <- Ref.of[IO, MessageId](MessageId.zero)
        heartbeat  <- Queue.unbounded[IO, Unit]
        orphanId   <- Ref.of[IO, Int](1)
        followerId <- Ref.of[IO, Int](1)
        leaderId   <- Ref.of[IO, Int](1)
        context = new DefaultNodeContext(
          config = config,
          nodeState = nodeState,
          messageId = messageId,
          heartbeat = heartbeat,
          orphanId = orphanId,
          followerId = followerId,
          leaderId = leaderId,
        )
        _ <- OperationService.run(context).supervise(supervisor)
        _ <- Server.instance(context, command).run
      } yield ()
    }

}

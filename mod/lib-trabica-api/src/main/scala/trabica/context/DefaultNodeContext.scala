package trabica.context

import cats.effect.std.Supervisor
import cats.effect.{IO, Ref}
import com.typesafe.config.{Config, ConfigFactory}
import trabica.fsm.{Node, StateMachine}
import trabica.model.*
import trabica.net.GrpcServer
import trabica.service.StateService

class DefaultNodeContext(
  val config: Config,
  val messageId: Ref[IO, MessageId],
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
        _         <- logging
        config    <- IO.blocking(ConfigFactory.load())
        nodeState <- StateService.state(command)
        messageId <- Ref.of[IO, MessageId](MessageId.zero)
        context = new DefaultNodeContext(
          config = config,
          messageId = messageId,
        )
        node   <- Ref.of[IO, Node](Node.dead)
        fsm    <- StateMachine.instance(context, node, supervisor)
        _      <- fsm.run.supervise(supervisor)
        state  <- nodeState.get
        _      <- fsm.events.offer(Event.NodeStateChanged(state))
        _      <- GrpcServer.resource(fsm, command).useForever
      } yield ()
    }

}

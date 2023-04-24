package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import fs2.*
import trabica.context.NodeContext
import trabica.model.{Event, NodeState}
import trabica.net.Server

object OperationService {

  def run(context: NodeContext): IO[Unit] =
    Supervisor[IO].use { supervisor =>
      for {
        _      <- eventStream(context, supervisor)
        _      <- operate(context, supervisor)
        server <- Server.instance(context).run
      } yield server
    }

  private def operate(context: NodeContext, supervisor: Supervisor[IO]): IO[Unit] =
    context.nodeState.get.flatMap {
      case state: NodeState.Orphan =>
        orphan(context, state, supervisor)
      case state: NodeState.NonVoter =>
        nonVoter(context, state, supervisor)
      case state: NodeState.Follower =>
        follower(context, state, supervisor)
      case state: NodeState.Candidate =>
        candidate(context, state, supervisor)
      case state: NodeState.Leader =>
        leader(context, state, supervisor)
      case state: NodeState.Joint =>
        joint(context, state, supervisor)
    }.void

  private def orphan(context: NodeContext, state: NodeState.Orphan, supervisor: Supervisor[IO]): IO[Unit] =
    JoinStream.run(context, state, supervisor)

  private def nonVoter(context: NodeContext, state: NodeState.NonVoter, supervisor: Supervisor[IO]): IO[Unit] =
    ???

  private def follower(context: NodeContext, state: NodeState.Follower, supervisor: Supervisor[IO]): IO[Unit] =
    ???

  private def candidate(context: NodeContext, state: NodeState.Candidate, supervisor: Supervisor[IO]): IO[Unit] =
    ???

  private def leader(context: NodeContext, state: NodeState.Leader, supervisor: Supervisor[IO]): IO[Unit] =
    HeartbeatStream.run(context, state, supervisor)

  private def joint(context: NodeContext, state: NodeState.Joint, supervisor: Supervisor[IO]): IO[Unit] =
    ???

  private def eventStream(context: NodeContext, supervisor: Supervisor[IO]): IO[Unit] =
    IO.println("[OperationService::eventStream] startup") >>
      Stream
        .fromQueueUnterminated(context.events)
        .evalTap(event => IO.println(s"[OperationService::eventStream] $event"))
        .evalMap {
          case Event.NodeStateEvent(nodeState) =>
            for {
              _ <- context.interrupt.set(true)
              _ <- context.nodeState.set(nodeState)
              _ <- context.interrupt.set(false)
              _ <- operate(context, supervisor)
            } yield ()
        }
        .compile
        .drain
        .supervise(supervisor)
        .void

}

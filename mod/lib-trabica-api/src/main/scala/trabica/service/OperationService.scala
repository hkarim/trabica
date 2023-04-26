package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import trabica.context.NodeContext
import trabica.model.NodeState

object OperationService {

  private final val logger = scribe.cats[IO]

  def run(context: NodeContext): IO[Unit] =
    operate(context)

  private def operate(context: NodeContext): IO[Unit] =
    Supervisor[IO](await = false).use { supervisor =>
      context.nodeState.get.flatMap {
        case _: NodeState.Orphan =>
          orphan(context, supervisor)
        case _: NodeState.NonVoter =>
          nonVoter(context)
        case _: NodeState.Follower =>
          follower(context, supervisor)
        case _: NodeState.Candidate =>
          candidate(context)
        case _: NodeState.Leader =>
          leader(context, supervisor)
        case _: NodeState.Joint =>
          joint(context)
      }
    }

  def stateChanged(context: NodeContext, state: NodeState): IO[Unit] =
    for {
      s <- context.nodeState.get
      _ <- logger.debug(s"1. state changing, current node state ${s.tag}")
      _ <- logger.debug(s"2. completing interrupt signal")
      r <- s.signal.complete(())
      _ <- logger.debug(s"3. signal done with value ($r), changing the state to ${state.tag}")
      _ <- logger.debug(s"4. state will be changed to ${state.tag}")
      _ <- context.nodeState.set(state)
      _ <- logger.debug(s"5. preparing operations")
      _ <- operate(context)
    } yield ()

  private def orphan(context: NodeContext, supervisor: Supervisor[IO]): IO[Unit] =
    for {
      state <- context.nodeState.get
      id    <- context.orphanId.getAndUpdate(_ + 1)
      _     <- logger.info(s"orphan [$id] starting")
      _     <- StateOrphanJoinStream.instance(context, id).run.supervise(supervisor)
      _     <- logger.info(s"orphan [$id] running, awaiting interrupt signal")
      _     <- state.signal.get
      _     <- logger.info(s"orphan [$id] interrupt signal received")
      _     <- logger.info(s"orphan [$id] ended")
    } yield ()

  private def nonVoter(context: NodeContext): IO[Unit] =
    ???

  private def follower(context: NodeContext, supervisor: Supervisor[IO]): IO[Unit] =
    for {
      state <- context.nodeState.get
      id    <- context.followerId.getAndUpdate(_ + 1)
      _     <- logger.info(s"follower [$id] starting").supervise(supervisor)
      // _ <- StateFollowerMonitorStream.run(context).supervise(supervisor)
      _ <- logger.info(s"follower [$id] running, awaiting interrupt signal")
      _ <- state.signal.get
      _ <- logger.info(s"follower [$id] interrupt signal received")
      _ <- logger.info(s"follower [$id] ended")
    } yield ()

  private def candidate(context: NodeContext): IO[Unit] =
    ???

  private def leader(context: NodeContext, supervisor: Supervisor[IO]): IO[Unit] =
    for {
      state <- context.nodeState.get
      id    <- context.leaderId.getAndUpdate(_ + 1)
      _     <- logger.info(s"leader [$id] starting")
      _     <- StateLeaderHeartbeatStream.instance(context, id).run.supervise(supervisor)
      _     <- logger.info(s"leader [$id] running, awaiting interrupt signal")
      _     <- state.signal.get
      _     <- logger.info(s"leader [$id] interrupt signal received")
      _     <- logger.info(s"leader [$id] ended")
    } yield ()

  private def joint(context: NodeContext): IO[Unit] =
    ???

}

package trabica.service

import cats.effect.*
import fs2.*
import trabica.context.NodeContext
import trabica.model.NodeState

import scala.concurrent.duration.*

object StateFollowerMonitorStream {

  private final val logger = scribe.cats[IO]

  def run(context: NodeContext): IO[Unit] =
    Stream
      .fixedRate[IO](10.second)
      .evalMap(_ => context.nodeState.get)
      .collect { case state: NodeState.Follower => state }
      .evalMap { state =>
        context
          .heartbeat
          .take
          .timeout(10.seconds)
          .handleErrorWith { _ =>
            def newState(signal: Deferred[IO, Unit]) = NodeState.Candidate(
              id = state.id,
              self = state.self,
              peers = state.peers,
              currentTerm = state.currentTerm.increment,
              votedFor = Some(state.self),
              commitIndex = state.commitIndex,
              lastApplied = state.lastApplied,
              signal = signal,
            )
            for {
              _      <- logger.info(s"follower heartbeat monitor timed out, switching state to candidate")
              signal <- Deferred[IO, Unit]
              _      <- OperationService.stateChanged(context, newState(signal))
            } yield ()
          }
      }
      .compile
      .drain
      .void

}

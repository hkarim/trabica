package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import fs2.*
import trabica.context.NodeContext
import trabica.model.{Event, NodeState}

import scala.concurrent.duration.*

object StateFollowerMonitorStream {

  private final val logger = scribe.cats[IO]

  def run(context: NodeContext, state: NodeState.Follower, supervisor: Supervisor[IO]): IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](1.second)
      .interruptWhen(context.interrupt)
      .evalMap { _ =>
        state.heartbeat
          .take
          .timeout(5.seconds)
          .handleErrorWith { _ =>
            val newState = NodeState.Candidate(
              id = state.id,
              self = state.self,
              peers = state.peers,
              currentTerm = state.currentTerm,
              votedFor = Some(state.self),
              commitIndex = state.commitIndex,
              lastApplied = state.lastApplied,
            )
            logger.info(s"follower heartbeat monitor timed out, switching state to candidate") >>
              context.events.offer {
                Event.NodeStateChangedEvent(newState)
              }
          }
      }
      .compile
      .drain
      .supervise(supervisor)
      .void

}

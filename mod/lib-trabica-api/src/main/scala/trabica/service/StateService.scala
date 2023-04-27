package trabica.service

import cats.effect.*
import cats.effect.std.UUIDGen
import trabica.model.*
import trabica.rpc.Peer

object StateService {

  private final val logger = scribe.cats[IO]

  def state(command: CliCommand): IO[Ref[IO, NodeState]] = command match {
    case v: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(v)
    case v: CliCommand.Join =>
      logger.debug("initiating node state in orphan mode") >>
        orphan(v)
  }

  private def bootstrap(command: CliCommand.Bootstrap): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](
      NodeState.Leader(
        id = id,
        self = Peer(
          host = command.host,
          port = command.port,
        ),
        peers = Set.empty,
        votedFor = None,
        currentTerm = 0L,
        commitIndex = 0L,
        lastApplied = 0L,
        nextIndex = 0L,
        matchIndex = 0L,
      )
    )
  } yield nodeState

  private def orphan(command: CliCommand.Join): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](
      NodeState.Orphan(
        id = id,
        self = Peer(
          host = command.host,
          port = command.port,
        ),
        peers = Set(
          Peer(
            host = command.peerHost,
            port = command.peerPort,
          )
        ),
        currentTerm = 0L,
        votedFor = None,
        commitIndex = 0L,
        lastApplied = 0L,
      )
    )
  } yield nodeState

}

package trabica.service

import cats.effect.*
import cats.effect.std.UUIDGen
import trabica.model.*

object StateService {

  def state(command: CliCommand): IO[Ref[IO, NodeState]] = command match {
    case v: CliCommand.Bootstrap =>
      bootstrap(v)
    case v: CliCommand.Join =>
      join(v)
  }

  private def bootstrap(command: CliCommand.Bootstrap): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](
      NodeState.Leader(
        id = id,
        self = Peer(
          ip = command.ip,
          port = command.port,
        ),
        peers = Set.empty,
        votedFor = None,
        currentTerm = Term.zero,
        commitIndex = Index.zero,
        lastApplied = Index.zero,
        nextIndex = Index.zero,
        matchIndex = Index.zero,
      )
    )
  } yield nodeState

  private def join(command: CliCommand.Join): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id   = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](
      NodeState.Orphan(
        id = id,
        self = Peer(
          ip = command.ip,
          port = command.port,
        ),
        peers = Set(
          Peer(
            ip = command.peerIp,
            port = command.peerPort,
          )
        ),
        currentTerm = Term.zero,
        votedFor = None,
        commitIndex = Index.zero,
        lastApplied = Index.zero
      )
    )
  } yield nodeState

}


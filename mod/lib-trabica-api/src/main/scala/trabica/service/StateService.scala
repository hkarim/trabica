package trabica.service

import cats.effect.*
import cats.effect.std.UUIDGen
import trabica.model.*

object StateService {

  private final val logger = scribe.cats[IO]

  def state(command: CliCommand): IO[Ref[IO, NodeState]] = command match {
    case v: CliCommand.Bootstrap =>
      logger.debug("initiating node state in bootstrap mode") >>
        bootstrap(v)
    case v: CliCommand.Join =>
      logger.debug("initiating node state in join mode") >>
        join(v)
  }

  private def bootstrap(command: CliCommand.Bootstrap): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    signal <- Deferred[IO, Unit]
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
        signal = signal,
      )
    )
  } yield nodeState

  private def join(command: CliCommand.Join): IO[Ref[IO, NodeState]] = for {
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    signal <- Deferred[IO, Unit]
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
        lastApplied = Index.zero,
        signal = signal,
      )
    )
  } yield nodeState

}

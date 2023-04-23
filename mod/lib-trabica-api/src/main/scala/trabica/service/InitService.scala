package trabica.service

import cats.effect.*
import cats.effect.std.UUIDGen
import com.comcast.ip4s.{Ipv4Address, Port}
import trabica.model.*

class InitService(command: CliCommand) {

  def state: IO[Ref[IO, NodeState]] = command match {
    case v: CliCommand.Bootstrap =>
      follower(v)
    case v: CliCommand.Join =>
      follower(v)
  }

  private def follower(command: CliCommand.Bootstrap): IO[Ref[IO, NodeState]] = for {
    ip   <- IO.fromOption(Ipv4Address.fromString(command.ip))(NodeError.InvalidConfig("trabica.node.ip"))
    port <- IO.fromOption(Port.fromInt(command.port))(NodeError.InvalidConfig("trabica.node.port"))
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](NodeState.Follower.fresh(id, ip, port, Vector.empty))
  } yield nodeState

  private def follower(command: CliCommand.Join): IO[Ref[IO, NodeState]] = for {
    ip       <- IO.fromOption(Ipv4Address.fromString(command.ip))(NodeError.InvalidConfig("trabica.node.ip"))
    port     <- IO.fromOption(Port.fromInt(command.port))(NodeError.InvalidConfig("trabica.node.port"))
    peerIp   <- IO.fromOption(Ipv4Address.fromString(command.peerIp))(NodeError.InvalidConfig("trabica.peer.ip"))
    peerPort <- IO.fromOption(Port.fromInt(command.peerPort))(NodeError.InvalidConfig("trabica.peer.port"))
    peer = Peer(peerIp, peerPort)
    uuid <- UUIDGen.randomUUID[IO]
    id = NodeId.fromUUID(uuid)
    nodeState <- Ref.of[IO, NodeState](NodeState.Follower.fresh(id, ip, port, Vector(peer)))
  } yield nodeState

}

object InitService {
  def instance(command: CliCommand): InitService =
    new InitService(command)
}

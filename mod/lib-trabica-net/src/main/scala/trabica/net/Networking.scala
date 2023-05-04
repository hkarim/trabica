package trabica.net

import cats.effect.{IO, Resource}
import trabica.model.{CliCommand, Peer}

trait Networking {
  
  def client(prefix: String, peer: Peer): Resource[IO, NodeApi]
  
  def server(api: NodeApi, command: CliCommand): Resource[IO, NodeApi]

}

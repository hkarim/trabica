package trabica.net

import cats.effect.{IO, Resource}
import trabica.model.Peer

trait Networking {
  
  def client(prefix: String, quorumId: String, quorumPeer: Peer): Resource[IO, NodeApi]
  
  def server(api: NodeApi, host: String, port: Int): Resource[IO, NodeApi]

}

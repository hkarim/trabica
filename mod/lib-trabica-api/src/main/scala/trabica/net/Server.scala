package trabica.net

import cats.effect.*
import fs2.*
import fs2.io.net.Network
import com.comcast.ip4s.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.{Request, Response}
import trabica.service.NodeService

class Server(nodeContext: NodeContext) {

  private final val leaderApi: NodeService = NodeService.instance(nodeContext)

  def run: IO[Unit] = {
    val ip      = Ipv4Address.fromString(nodeContext.config.getString("trabica.node.ip"))
    val port    = Port.fromInt(nodeContext.config.getInt("trabica.node.port"))
    Network[IO].server(ip, port).map { client =>
      client.reads
        .through(StreamDecoder.many(Decoder[Request]).toPipeByte)
        .evalMap(leaderApi.onRequest)
        .through(StreamEncoder.many(Encoder[Response]).toPipeByte)
        .through(client.writes)
        .handleErrorWith { e =>
          println(s"server stream error: ${e.getMessage}")
          e.printStackTrace()
          Stream.empty
        }
    }.parJoin(100).compile.drain
  }
}

object Server {
  def instance(nodeContext: NodeContext): Server =
    new Server(nodeContext)
}

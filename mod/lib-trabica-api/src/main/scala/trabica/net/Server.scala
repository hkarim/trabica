package trabica.net

import cats.effect.*
import fs2.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.{Request, Response}
import trabica.service.NodeService

class Server(context: NodeContext) {

  private final val nodeService: NodeService = NodeService.instance(context)

  def run: IO[Unit] =
    IO.println("[Server] startup") >>
      context.nodeState.get.flatMap { nodeState =>
        val ip   = Some(nodeState.self.ip)
        val port = Some(nodeState.self.port)
        Network[IO].server(ip, port).map { client =>
          client.reads
            .through(StreamDecoder.many(Decoder[Request]).toPipeByte)
            .evalMap(nodeService.onRequest)
            .through(StreamEncoder.many(Encoder[Response]).toPipeByte)
            .through(client.writes)
            .handleErrorWith { e =>
              println(s"server stream error: ${e.getMessage}")
              e.printStackTrace()
              Stream.empty
            }
        }
          .parJoinUnbounded
          .compile
          .drain
      }

}

object Server {
  def instance(nodeContext: NodeContext): Server =
    new Server(nodeContext)
}

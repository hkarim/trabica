package trabica.net

import cats.effect.*
import fs2.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.{Request, Response}
import trabica.service.RouterService

class Server(context: NodeContext) {

  private final val logger = scribe.cats[IO]

  private final val routerService: RouterService = RouterService.instance(context)

  def run: IO[Unit] =
    context.nodeState.get.flatMap { nodeState =>
      val ip   = Some(nodeState.self.ip)
      val port = Some(nodeState.self.port)
      logger.info(s"starting up main communication server on ip: ${nodeState.self.ip}, port: ${nodeState.self.port}") >>
        Network[IO].server(ip, port).map { client =>
          client.reads
            .through(StreamDecoder.many(Decoder[Request]).toPipeByte)
            .evalMap(routerService.onRequest)
            .through(StreamEncoder.many(Encoder[Response]).toPipeByte)
            .through(client.writes)
            .handleErrorWith { e =>
              Stream.eval(logger.error(s"server stream error: ${e.getMessage}", e)) >>
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

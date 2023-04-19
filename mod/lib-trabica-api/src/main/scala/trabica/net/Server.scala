package trabica.net

import cats.effect.*
import io.circe.*
import io.circe.syntax.*
import fs2.*
import fs2.io.net.Network
import com.comcast.ip4s.*
import trabica.context.NodeContext
import trabica.model.Request
import trabica.service.NodeService

class Server(nodeContext: NodeContext) {

  private final val leaderApi: NodeService = NodeService.instance(nodeContext)

  def run: IO[Unit] = {
    val ip   = Ipv4Address.fromString(nodeContext.config.getString("trabica.node.ip"))
    val port = Port.fromInt(nodeContext.config.getInt("trabica.node.port"))
    Network[IO].server(ip, port).map { client =>
      client.reads
        .through(text.utf8.decode)
        .through(text.lines)
        .evalTap { line =>
          IO.println(s"[request] $line")
        }
        .evalMap { line =>
          IO.fromEither(parser.parse(line))
        }
        .evalMap { json =>
          IO.fromEither(json.as[Request])
        }
        .evalMap(leaderApi.onRequest)
        .map { message =>
          s"${message.asJson.noSpaces}\n"
        }
        .evalTap { line =>
          IO.println(s"[response] $line")
        }
        .through(text.utf8.encode)
        .through(client.writes)
        .handleErrorWith { e =>
          println(s"server stream error: ${e.getMessage}")
          Stream.empty
        }
    }.parJoin(100).compile.drain
  }
}

object Server {
  def instance(nodeContext: NodeContext): Server =
    new Server(nodeContext)
}

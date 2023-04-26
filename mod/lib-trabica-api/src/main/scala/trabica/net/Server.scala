package trabica.net

import cats.effect.*
import fs2.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network
import scodec.{Decoder, Encoder}
import trabica.context.NodeContext
import trabica.model.{CliCommand, Header, Request, Response}
import trabica.service.RouterService

class Server(context: NodeContext, command: CliCommand) {

  private final val logger = scribe.cats[IO]

  private final val routerService: RouterService = RouterService.instance(context)

  def run: IO[Unit] =
    logger.info(s"starting up main communication server on ip: ${command.ip}, port: ${command.port}") >>
      Network[IO]
        .server(Some(command.ip), Some(command.port))
        .evalTap { client =>
          for {
            ra <- client.remoteAddress
            _ <- logger.debug(s"client ${ra} connected")
          } yield ()
        }
        .map { client =>
          client.reads
            .through(StreamDecoder.many(Decoder[Request]).toPipeByte)
            .evalTap(_ => logger.debug(s"server request received"))
            .evalMap { request =>
              routerService
                .onRequest(request)
                .handleErrorWith { e =>
                  for {
                    _         <- logger.error(s"error handling request downstream, will return an error reply to the client", e)
                    messageId <- context.messageId.getAndUpdate(_.increment)
                    s         <- context.nodeState.get
                    response = Response.Error(
                      header = Header(
                        peer = s.self,
                        messageId = messageId,
                        term = s.currentTerm,
                      )
                    )
                  } yield response
                }
            }
            .through(StreamEncoder.many(Encoder[Response]).toPipeByte)
            .through(client.writes)
            .handleErrorWith { e =>
              Stream.eval {
                for {
                  _ <- logger.error(s"server stream error: ${e.getMessage}", e)
                } yield ()
              } >> Stream.empty
            }
        }
        .parJoinUnbounded
        .compile
        .drain

}

object Server {
  def instance(context: NodeContext, command: CliCommand): Server =
    new Server(context, command)
}

package trabica.net

import cats.effect.*
import fs2.grpc.syntax.all.*
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.{Metadata, Server}
import trabica.rpc.TrabicaFs2Grpc

object GrpcServer {

  private final val logger = scribe.cats[IO]

  def resource(handler: TrabicaFs2Grpc[IO, Metadata], port: Int): Resource[IO, Server] = {
    def acquire(server: Server): IO[Server] =
      IO.delay(server.start())

    def release(server: Server): IO[Unit] =
      // IO.blocking(server.awaitTermination())
      IO.blocking(server.shutdownNow()).void

    TrabicaFs2Grpc
      .bindServiceResource[IO](handler).flatMap { ssd =>
        NettyServerBuilder
          .forPort(port)
          .addService(ssd)
          .resource[IO]
          .evalTap { _ =>
            logger.debug(s"grpc server starting on port ${port}")
          }
          .flatMap { server =>
            Resource.make(acquire(server))(release)
          }
          .onFinalize {
            logger.debug(s"grpc server shutdown")
          }
      }
  }
}

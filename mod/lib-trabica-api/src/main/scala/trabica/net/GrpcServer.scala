package trabica.net

import cats.effect.*
import fs2.grpc.syntax.all.*
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import trabica.fsm.StateMachine
import trabica.model.CliCommand
import trabica.rpc.TrabicaFs2Grpc

object GrpcServer {
  def resource(fsm: StateMachine, command: CliCommand): Resource[IO, Server] = {
    def acquire(server: Server): IO[Server] =
      IO.delay(server.start())

    def release(server: Server): IO[Unit] =
      //IO.blocking(server.awaitTermination())
      IO.blocking(server.shutdownNow()).void

    TrabicaFs2Grpc
      .bindServiceResource[IO](fsm).flatMap { ssd =>
        NettyServerBuilder
          .forPort(command.port)
          .addService(ssd)
          .resource[IO]
          .flatMap { server =>
            Resource.make(acquire(server))(release)
          }
      }
  }
}

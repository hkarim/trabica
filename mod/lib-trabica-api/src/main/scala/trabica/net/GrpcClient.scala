package trabica.net

import cats.effect.{IO, Resource}
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import fs2.grpc.syntax.all.*
import trabica.rpc.*

object GrpcClient {

  private final val logger = scribe.cats[IO]

  def forPeer(peer: Peer): Resource[IO, TrabicaFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(peer.host, peer.port)
      .usePlaintext()
      .resource[IO]
      .flatMap { channel =>
        TrabicaFs2Grpc.stubResource[IO](channel)
      }
      .onFinalize {
        logger.debug(s"grpc client for peer ${peer.host}:${peer.port} closed")
      }

}

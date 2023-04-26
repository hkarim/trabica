package trabica.service

import cats.effect.*
import cats.effect.std.Supervisor
import cats.syntax.all.*
import fs2.grpc.syntax.all.*
import io.grpc.{Metadata, Server}
import io.grpc.netty.shaded.io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import trabica.context.NodeContext
import trabica.model.Peer
import trabica.rpc.*

object Scratch {

  object Rpc extends TrabicaFs2Grpc[IO, Metadata] {
    def appendEntries(request: AppendEntriesRequest, metadata: Metadata): IO[AppendEntriesResponse] = ???
  }

  def run(context: NodeContext): IO[Unit] =
    operate(context)

  class Follower {
    def run: IO[Unit] = ???
  }
  object Follower {
    def resource(context: NodeContext, clients: Vector[TrabicaFs2Grpc[IO, Metadata]]): Resource[IO, Follower] =
      ???
  }

  def server(port: Int): Resource[IO, Server] =
    TrabicaFs2Grpc
      .bindServiceResource[IO](Rpc).flatMap { ssd =>
      NettyServerBuilder
        .forPort(port)
        .addService(ssd)
        .resource[IO]
        .evalMap { server =>
          IO.delay {
            server.start()
          }
        }
    }


  private def client(peer: Peer): Resource[IO, TrabicaFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(peer.ip.toString, peer.port.value)
      .resource[IO]
      .flatMap { channel =>
        TrabicaFs2Grpc.stubResource[IO](channel)
      }

  private def operate(context: NodeContext): IO[Unit] =
    Supervisor[IO].use { supervisor =>
      val resource = for {
        state <- Resource.eval(context.nodeState.get)
        clients <- state.peers.toVector.traverse { peer =>
          client(peer)
        }
        result <- Follower.resource(context, clients)
      } yield result

      resource.use { engine =>
        for {
          state <- context.nodeState.get
          _ <- engine.run.supervise(supervisor)
          _ <- state.signal.get
        } yield ()
      }
    }

}

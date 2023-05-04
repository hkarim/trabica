package trabica.net
import cats.effect.{IO, Resource}
import io.grpc.Metadata
import trabica.model.*
import trabica.rpc.TrabicaFs2Grpc

object Grpc extends Networking {


  private class GrpcClientNodeApi(val peer: Peer, client: TrabicaFs2Grpc[IO, Metadata]) extends NodeApi {

    override def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse] =
      client.appendEntries(request, new Metadata)

    override def vote(request: VoteRequest): IO[VoteResponse] =
      client.vote(request, new Metadata)

    override def join(request: JoinRequest): IO[JoinResponse] =
      client.join(request, new Metadata)

  }

  private class GrpcServerNodeApi(server: NodeApi) extends TrabicaFs2Grpc[IO, Metadata] {

    override def appendEntries(request: AppendEntriesRequest, ctx: Metadata): IO[AppendEntriesResponse] =
      server.appendEntries(request)

    override def vote(request: VoteRequest, ctx: Metadata): IO[VoteResponse] =
      server.vote(request)

    override def join(request: JoinRequest, ctx: Metadata): IO[JoinResponse] =
      server.join(request)
  }

  def client(prefix: String, peer: Peer): Resource[IO, NodeApi] =
    GrpcClient
      .forPeer(prefix, peer)
      .map(c => new GrpcClientNodeApi(peer, c))

  def server(api: NodeApi, command: CliCommand): Resource[IO, NodeApi] =
    GrpcServer
      .resource(new GrpcServerNodeApi(api), command.port)
      .map(_ => api)
}

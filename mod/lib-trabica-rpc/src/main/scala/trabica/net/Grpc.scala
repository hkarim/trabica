package trabica.net
import cats.effect.{IO, Resource}
import io.grpc.Metadata
import trabica.model.*
import trabica.rpc.TrabicaFs2Grpc

object Grpc extends Networking {

  private class GrpcClientNodeApi(val quorumId: String, val quorumPeer: Peer, client: TrabicaFs2Grpc[IO, Metadata]) extends NodeApi {

    override def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse] =
      client.appendEntries(request, new Metadata)

    override def vote(request: VoteRequest): IO[VoteResponse] =
      client.vote(request, new Metadata)
  }

  private class GrpcServerNodeApi(server: NodeApi) extends TrabicaFs2Grpc[IO, Metadata] {

    override def appendEntries(request: AppendEntriesRequest, ctx: Metadata): IO[AppendEntriesResponse] =
      server.appendEntries(request)

    override def vote(request: VoteRequest, ctx: Metadata): IO[VoteResponse] =
      server.vote(request)
  }

  override def client(prefix: String, quorumId: String, quorumPeer: trabica.model.Peer): Resource[IO, NodeApi] =
    GrpcClient
      .forPeer(prefix, quorumPeer)
      .map(c => new GrpcClientNodeApi(quorumId, quorumPeer, c))

  override def server(api: NodeApi, host: String, port: Int): Resource[IO, NodeApi] =
    GrpcServer
      .resource(new GrpcServerNodeApi(api), port)
      .map(_ => api)
}

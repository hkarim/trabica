package trabica.fsm

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.grpc.Metadata
import fs2.*
import trabica.context.NodeContext
import trabica.model.NodeState
import trabica.net.GrpcClient
import trabica.rpc.{Header, JoinRequest, JoinResponse, TrabicaFs2Grpc}
import trabica.service.StateOrphanService

import scala.concurrent.duration.*

class OrphanNode(
  val context: NodeContext,
  val state: Ref[IO, NodeState],
  val events: Queue[IO, NodeState],
  val signal: Deferred[IO, Unit],
  val supervisor: Supervisor[IO]
) extends Node {

  private final val logger = scribe.cats[IO]

  override final val server: TrabicaFs2Grpc[IO, Metadata] =
    StateOrphanService.instance(context, events)

  override def interrupt: IO[Unit] =
    signal.complete(()).void

  private def clients: Resource[IO, Vector[TrabicaFs2Grpc[IO, Metadata]]] =
    for {
      s <- Resource.eval(state.get)
      clients <- s.peers.toVector.traverse { peer =>
        GrpcClient.forPeer(peer)
      }
    } yield clients

  private def joinStream: IO[Unit] =
    Stream
      .fixedRateStartImmediately[IO](2.seconds)
      .flatMap { _ =>
        Stream
          .resource(clients)
          .evalMap { cs =>
            for {
              messageId <- context.messageId.getAndUpdate(_.increment)
              s         <- state.get
              request = JoinRequest(
                header = Header(
                  peer = s.self.some,
                  messageId = messageId.value,
                  term = s.currentTerm,
                ).some
              )
              responses <- cs.parTraverse { c =>
                c.join(request, new Metadata)
                  .attempt
                  .timeout(100.milliseconds)
              }
            } yield responses
          }
          .evalMap(_.parTraverse(onResponse))
      }
      .compile
      .drain

  private def onResponse(response: Either[Throwable, JoinResponse]): IO[Unit] =
    response match {
      case Left(e) =>
        logger.debug(s"no response ${e.getMessage}")
      case Right(value) =>
        logger.debug(s"response $value")
    }

  def run: IO[Unit] = joinStream

}

object OrphanNode {
  def instance(
    context: NodeContext,
    state: Ref[IO, NodeState],
    events: Queue[IO, NodeState],
    signal: Deferred[IO, Unit],
    supervisor: Supervisor[IO]
  ): OrphanNode =
    new OrphanNode(
      context,
      state,
      events,
      signal,
      supervisor
    )
}

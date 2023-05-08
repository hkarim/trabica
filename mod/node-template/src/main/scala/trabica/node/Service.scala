package trabica.node

import cats.effect.*
import cats.effect.std.Supervisor
import com.google.protobuf.ByteString
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
import trabica.model.{CliCommand, LogEntry, LogEntryTag}
import trabica.net.Grpc
import trabica.store.{FsmFileStore, FsmStore}

import scala.concurrent.duration.*

object Service
  extends CommandIOApp(name = "trabica-node", header = "trabica node", version = "0.0.1") {

  private val logger = scribe.cats[IO]

  private def feed(store: FsmStore): IO[Unit] =
    fs2.Stream
      .range(2, 20, 1)
      .delayBy[IO](2.seconds)
      .metered[IO](100.milliseconds)
      .evalMap { index =>
        val entry = LogEntry(
          index = index,
          term = 1L,
          tag = LogEntryTag.Data,
          data = ByteString.copyFromUtf8("hello raft")
        )
        store.append(entry)
      }
      .evalTap(result => logger.debug(s"[main] entry appended -> $result"))
      .compile
      .drain

  override def main: Opts[IO[ExitCode]] =
    CliCommand.parse.map { command =>
      Supervisor[IO](await = false).use { supervisor =>
        FsmFileStore.resource(command.dataDirectory).use { store =>
          val feedIO = command match {
            case _: CliCommand.Bootstrap =>
              feed(store)
            case _: CliCommand.Startup =>
              IO.unit
          }
          for {
            _ <- feedIO.supervise(supervisor)
            _ <- Trabica.run(supervisor, command, store, Grpc).guarantee {
              store
                .stream
                .evalTap(IO.println)
                .compile
                .drain
            }
          } yield ExitCode.Success
        }
      }

    }
}

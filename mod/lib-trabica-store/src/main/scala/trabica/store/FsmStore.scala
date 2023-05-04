package trabica.store

import cats.effect.*
import fs2.*
import trabica.model.{Index, LogEntry}

trait FsmStore {
  def configuration: IO[Option[LogEntry]]
  def stream: Stream[IO, LogEntry]
  def streamFrom(index: Index): Stream[IO, LogEntry]
  def atIndex(index: Index): IO[LogEntry]
  def append(entry: LogEntry): IO[Unit]
  def truncate(keepIndex: Index): IO[Unit]
}
